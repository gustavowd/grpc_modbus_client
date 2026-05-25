use tonic::Request;
use tokio_stream::StreamExt; // Necessário para usar o .next() no stream

// Imports adicionais necessários para o conector Unix
use tokio::net::UnixStream;
use tokio::time::Duration;
use tower::service_fn;
use tonic::transport::{Endpoint, Uri};

// Autogerado pelo tonic-build a partir do mesmo arquivo .proto
pub mod sunspec_grpc {
    tonic::include_proto!("sunspec.telemetry.v1");
}

use sunspec_grpc::sun_spec_telemetry_service_client::SunSpecTelemetryServiceClient;
use sunspec_grpc::equipment_data_response::ModelData;
use sunspec_grpc::StreamRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
// -----------------------------------------------------------------
    // NOVA LOGICA: Configurando a conexão via Unix Domain Socket
    // -----------------------------------------------------------------
    let socket_path = "/tmp/sunspec.sock";
    println!("Conectando ao servidor de telemetria via UDS em {}...", socket_path);
    
    // O Tonic precisa de uma URL formalmente válida para o Endpoint, 
    // mas a função "service_fn" abaixo vai ignorá-la e forçar o uso do socket.
    // O Tonic precisa de um URI formalmente válido, mas o conector customizado vai ignorá-lo.
    loop{
        let channel: tonic::transport::Channel;

        // Sub-loop para tentar conectar até o arquivo do socket estar disponível
        loop {
            println!("Tentando conectar ao servidor gRPC via UDS em {}...", socket_path);

            let endpoint_result = Endpoint::try_from("http://[::]:50051")?
                .connect_with_connector(service_fn(move |_: Uri| {
                    async move {
                        let stream = UnixStream::connect(socket_path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::tokio::TokioIo::new(stream))
                    }
                })).await;

            match endpoint_result {
                Ok(chan) => {
                    channel = chan;
                    println!("🔌 Conectado com sucesso via canal IPC local!");
                    break; // Conectou! Sai deste sub-loop e vai para o streaming
                }
                Err(e) => {
                    eprintln!("Falha ao conectar: {}. Nova tentativa em 3 segundos...", e);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }

        let mut client = SunSpecTelemetryServiceClient::new(channel);
        println!("Conectado com sucesso via canal IPC local!");

        // 2. Prepara a requisição. Você poderia passar um ID específico aqui.
        let request = Request::new(StreamRequest {
            equipment_id: "".to_string(), // Vazio = receber de todos os equipamentos
        });

        // 3. Chama o método de streaming e recebe o canal de entrada
        let mut stream = client
            .stream_equipment_updates(request)
            .await?
            .into_inner(); // <-- ISSO EXTRAI O STREAM REAL DE DENTRO DA RESPOSTA!

        println!("Aguardando dados dos inversores/medidores SunSpec...\n");

        // 4. Loop assíncrono para processar cada mensagem recebida em tempo real
        while let Some(response_result) = stream.next().await {
            match response_result {
                Ok(data) => {
                    println!("--- Nova leitura recebida [ID: {}] ---", data.equipment_id);
                    println!("Timestamp (ms): {}", data.timestamp_ms);

                    // Fazemos o match dinâmico no oneof. O Rust nos obriga a tratar
                    // cada variante que pode vir dentro do container "model_data"
                    if let Some(model_data) = data.model_data {
                        match model_data {
                            // Variante do Modelo 1 (Common)
                            ModelData::CommonData(common) => {
                                println!("[Modelo 1] Dados de Identificação recebidos:");
                                println!("  -> Fabricante: {} | Modelo: {}", common.manufacturer, common.model);
                                println!("  -> Número de Série: {} | Versão: {}", common.serial_number, common.version);
                            },
                            
                            // Variante do Modelo 213 (Meter)
                            ModelData::MeterData(meter) => {
                                println!("[Modelo 213] Medidor Trifásico recebido:");
                                println!("  -> Corrente Total: {:.2} A", meter.ampers);
                                println!("  -> Linha A: {:.2} A | Linha B: {:.2} A | Linha C: {:.2} A", 
                                    meter.ampers_phase_a, meter.ampers_phase_b, meter.ampers_phase_c);
                                println!("  -> Tensão Média: {:.1} V", meter.voltage_ln);
                                println!("  -> Potência Ativa: {:.2} W (kW: {:.2})", meter.real_power, meter.real_power / 1000.0);
                            },
                            ModelData::InverterData(_inverter) => {
                                println!("[Modelo 101] Inversor recebido:");
                            }
                        }
                    } else {
                        println!("Aviso: Pacote recebido sem nenhum dado de modelo associado.");
                    }
                    
                    println!("--------------------------------------------------\n");
                },
                Err(status) => {
                    eprintln!("Erro no fluxo de dados enviado pelo servidor: {}", status);
                    break;
                }
            }
        }

        println!("Fluxo encerrado pelo servidor.");
    }
    //Ok(())
}
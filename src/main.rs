use tonic::Request;
use tokio_stream::StreamExt; // Necessário para usar o .next() no stream

// Autogerado pelo tonic-build a partir do mesmo arquivo .proto
pub mod sunspec_grpc {
    tonic::include_proto!("sunspec.telemetry.v1");
}

use sunspec_grpc::sun_spec_telemetry_service_client::SunSpecTelemetryServiceClient;
use sunspec_grpc::equipment_data_response::ModelData;
use sunspec_grpc::StreamRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Conecta ao servidor gRPC que criamos anteriormente
    let server_addr = "http://127.0.0.1:50051";
    println!("Conectando ao servidor de telemetria em {}...", server_addr);
    
    let mut client = SunSpecTelemetryServiceClient::connect(server_addr).await?;
    println!("Conectado com sucesso!");

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
    Ok(())
}
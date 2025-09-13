using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

internal class Program
{
    private static readonly string connectionString =
        @"Data Source=DESKTOP-MR57OTF;Initial Catalog=BASE_CLIENTES;User ID=sa;Password=Abisoft2024;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";

    private static readonly string postUrl = "https://lambda.rigel-m.com/pandora/v1/generateOrder";
    private const int BatchSize = 500; // puedes ajustar

    private static async Task Main(string[] args)
    {
        var httpClient = new HttpClient();

        long totalRegistros = await ObtenerTotalPendientes();
        long procesados = 0;

        Console.WriteLine($"Iniciando procesamiento. Total pendientes: {totalRegistros}");

        bool quedanRegistros = true;
        while (quedanRegistros)
        {
            int procesadosLote = await ProcesarLote(httpClient);
            if (procesadosLote == 0)
            {
                quedanRegistros = false;
            }
            else
            {
                procesados += procesadosLote;
                Console.WriteLine($"Procesados: {procesados} de {totalRegistros} ({DateTime.Now})");
            }
        }

        Console.WriteLine("Procesamiento completado ✅");
    }

    private static async Task<long> ObtenerTotalPendientes()
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        string sql = "SELECT COUNT(*) FROM TelefonosMovi WHERE procesado IS NULL OR procesado=0";
        using var cmd = new SqlCommand(sql, connection);
        return (int)await cmd.ExecuteScalarAsync();
    }

    private static async Task<int> ProcesarLote(HttpClient httpClient)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        string sqlSelect = $@"
            SELECT TOP ({BatchSize}) Id, Telefono, CEDULA_NUM
            FROM TelefonosMovi
            WHERE procesado IS NULL OR procesado=0
            ORDER BY Id";

        using var command = new SqlCommand(sqlSelect, connection);
        using var reader = await command.ExecuteReaderAsync();

        if (!reader.HasRows) return 0;

        var registros = new List<(int Id, string Telefono, string Cedula)>();
        while (await reader.ReadAsync())
        {
            registros.Add((reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
        }

        foreach (var (id, numero, cedula) in registros)
        {
            string jsonRespuesta;

            try
            {
                var payload = new
                {
                    cellphone = numero,
                    identification = cedula,
                    plan_code = "P0252",
                    utm_source = "web",
                    utm_medium = "web",
                    utm_campaing = "UPSELLPANDORA",
                    utm_content = "UPSELLPANDORA",
                    source = "PlanesMovistar-PlanesMovistar-Home-Card-Posicion3",
                    tsource = "",
                    tokenCaptcha = "0cAFcWeA6PBS3ZgeMgxnGOCPyJVwNEEm1CgB8K6EKlZWTBJCPteNwXEHjfNvJ9LBm88_tTSh0Uqg_n_hCZ6T572aSCZyD4Rx6DXERkDiNTvTkAK6UnvRLC6zqHwZtGDiBDIbwyd937quYGiuRv_554xuBCa1eYEWj7WeXIdiJAO7srIWCwkuOdbCnphdvp2raHmEwF7zPcKSvMDVAy2_DYrXfl7a4rooh3LYQ-dx-DPBobE1Ezs-O6ppFYa8QhtIoygvnOOdpm5Lg-PVMBLxVstmckEA2I_ZKnj-2o6k_Ug6xXOLUHnebKwNRRW0TsgKVoaMCv_9CREK-kyiPeXn9SeOXky8fUSh-9OpZCOfdpRy4Bxmw-rd6aHh5J1_-uNqVXOOK_DMRBg2P9IrrkKYCNEuZFVeM_IEWNrymjS-sm6w4TQYql3xKksrThEQ9NKhOxupPtQdoA9deMB55ITZFFC4-r9IvGUldQ0eKzGSsHrIjSmkhUmq0xYV2CoM8yct5kfHKAB4N6mQ6DH2eAmxsnbZ5A1WZkYuJFNGGzSolqQUuDSu1sX6LWnVQlNUePhZhIVISh3dlTOhfF7LhvHSIXOIeNzx2sy5aiWZsDOzLOI8E1QrhLG4MauuxFurd9JYcAOopjsp-2PUki-njkbCPK9ZyWLe9PDDwUEGEzbpKmalI_MlK-BEgFxC8H5y3-7NZkiv8ZkdwZO3KNSto2ihtu0nKjT73mdSfrPnVddxMKgXHQ4lvFySb7IFboI1UWYBCNPDPTzgJOVheNE2ZlQxw7qfH8Swp0ayN5iUK_dBeKweLcWNF69J1JAJ8tIGGhlcf4ta8PcvyrvYvTKAQ54J8Ph3yetj2xrAFxQw",
                    documentReferrer = ""
                };

                string jsonBody = System.Text.Json.JsonSerializer.Serialize(payload);
                var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

                var response = await httpClient.PostAsync(postUrl, content);
                if (response.IsSuccessStatusCode)
                {
                    // ✅ Respuesta válida (200, 201…)
                    jsonRespuesta = await response.Content.ReadAsStringAsync();
                    await GuardarJsonEnBD(id, jsonRespuesta, numero);
                }
                else
                {
                    // ⚠️ Error (401, 500, etc.)
                    //string errorContent = await response.Content.ReadAsStringAsync();
                    //jsonRespuesta = $@"{{
                    //    ""error"": true,
                    //    ""statusCode"": {(int)response.StatusCode},
                    //    ""reasonPhrase"": ""{response.ReasonPhrase}"",
                    //    ""body"": {System.Text.Json.JsonSerializer.Serialize(errorContent)}
                    //}}";
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"⚠️ Error {response.StatusCode} ({response.ReasonPhrase}) para número {numero}");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error consultando {numero}: {ex.Message}");
                jsonRespuesta = $"{{\"error\":\"Exception\",\"message\":\"{ex.Message}\"}}";
            }
        }

        return registros.Count;
    }

    private static async Task GuardarJsonEnBD(int id, string json, string numero)
    {
        string mensaje = null;
        string estado = null;

        try
        {
            var j = JObject.Parse(json);

            mensaje = j.SelectToken("message")?.ToString()
                    ?? j.SelectToken("dataResponse.systemmessage")?.ToString();

            estado = j.SelectToken("dataResponse.id_respuesta")?.ToString()
                   ?? j.SelectToken("id_respuesta")?.ToString();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parseando JSON para {numero}: {ex.Message}");
        }

        using var connection1 = new SqlConnection(connectionString);
        await connection1.OpenAsync();

        string sql = @"UPDATE TelefonosMovi
                       SET response = @json,
                           mensaje = @mensaje,
                           estado = @estado,
                           Fecha = GETDATE(),
                           procesado = 1
                       WHERE Id = @id";

        using var updateCmd = new SqlCommand(sql, connection1);
        updateCmd.Parameters.AddWithValue("@json", json);
        updateCmd.Parameters.AddWithValue("@mensaje", (object?)mensaje ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@estado", (object?)estado ?? DBNull.Value);
        updateCmd.Parameters.AddWithValue("@id", id);
        await updateCmd.ExecuteNonQueryAsync();

        Console.WriteLine($"Telefono {numero} actualizado {DateTime.Now}");
    }
}
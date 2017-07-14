using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;

namespace SearchAzureStorageLogs
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        private static async Task MainAsync(string[] args)
        {
            await Task.Yield();

            var logs = Directory
                .EnumerateFiles(@"D:\trash\2017-07-12", "*.log", SearchOption.AllDirectories)
                .ToList();

            using (var outputStream = new FileStream("output.csv", FileMode.Create, FileAccess.Write))
            using (var streamWriter = new StreamWriter(outputStream))
            using (var csvWriter = new CsvWriter(streamWriter))
            {
                csvWriter.WriteField("Transaction Time");
                csvWriter.WriteField("Operation Type");
                csvWriter.WriteField("Authentication Type");
                csvWriter.WriteField("Client IP and Port");
                csvWriter.WriteField("Client IP");
                csvWriter.WriteField("Object Key");
                csvWriter.WriteField("User Agent");
                csvWriter.WriteField("Referrer");
                csvWriter.NextRecord();

                foreach (var path in logs)
                {
                    Console.WriteLine(path);
                    using (var inputStream = File.OpenRead(path))
                    using (var streamReader = new StreamReader(inputStream))
                    using (var csvReader = new CsvReader(streamReader))
                    {
                        csvReader.Configuration.Delimiter = ";";
                        while (csvReader.Read())
                        {
                            var objectKey = csvReader.GetField<string>(12);
                            if (!objectKey.Contains("/v3-stats0/"))
                            {
                                continue;
                            }

                            var transactionTime = csvReader.GetField<string>(1);
                            var operationType = csvReader.GetField<string>(2);
                            var authenticationType = csvReader.GetField<string>(8);
                            var clientIpAndPort = csvReader.GetField<string>(15);
                            var clientIp = clientIpAndPort.Split(':')[0];
                            var userAgent = csvReader.GetField<string>(27);
                            var referrer = csvReader.GetField<string>(28);

                            csvWriter.WriteField(transactionTime);
                            csvWriter.WriteField(operationType);
                            csvWriter.WriteField(authenticationType);
                            csvWriter.WriteField(clientIpAndPort);
                            csvWriter.WriteField(clientIp);
                            csvWriter.WriteField(objectKey);
                            csvWriter.WriteField(userAgent);
                            csvWriter.WriteField(referrer);
                            csvWriter.NextRecord();
                        }
                    }

                    streamWriter.Flush();
                }
            }
        }
    }
}

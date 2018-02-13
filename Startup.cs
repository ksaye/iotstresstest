using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace IoTHubStressClient
{
    public class Startup
    {
        static string commonKey = "GDX4tNsIpZXa3+c8OnMqhI2Vc4ToRu2yUvHU+FK71mw=";
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            app.UseWebSockets();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.Run(async (context) =>
            {
                try
                {
                    // getting the passed parameters
                    string iotHubConnectionString = context.Request.Query["iotHubConnectionString"];
                    JObject payload = JObject.Parse(context.Request.Query["payload"]);
                    int clientCount = Convert.ToInt16(context.Request.Query["clientCount"]);
                    int messageCount = Convert.ToInt16(context.Request.Query["messageCount"]);
                    int messageDelaySeconds = Convert.ToInt16(context.Request.Query["messageDelaySeconds"]);
                    bool randomize = Convert.ToBoolean(context.Request.Query["randomize"]);

                    await context.Response.WriteAsync("Starting: " + clientCount + " clients<br>");
                    await context.Response.WriteAsync("-->Sending Message: " + payload + " " + messageCount + " times<br>");
                    await context.Response.WriteAsync("-->Every : " + messageDelaySeconds + " seconds <br>");
                    if (randomize)
                    {
                        await context.Response.WriteAsync("--->With time randomization <br>");
                    }
                    else {
                        await context.Response.WriteAsync("--->Without time randomization <br>");
                    }

                    await IoTClientTest(iotHubConnectionString,
                                        payload,
                                        clientCount,
                                        messageCount,
                                        messageDelaySeconds,
                                        randomize);
                }
                catch (Exception er) {
                    await context.Response.WriteAsync("Error: " + er.Message + " "+ er.InnerException + "<br>");
                    await context.Response.WriteAsync("<br>Expected a URL like: ?iotHubConnectionString=HostName%3Dksayestresstest.azure-devices.net%3BSharedAccessKeyName%3Diothubowner%3BSharedAccessKey%3DDb2CYoyXiaD4Yf37HszI1TrUoc0xrhtfy5Qoh3QQLBs%3D&payload=%7B%22messageType%22%3A%22status%22%7D&clientCount=100&messageCount=10&messageDelaySeconds=2&randomize=True");
                }
            });
        }
        private async Task IoTClientTest(string iotHubConnectionString, 
                                        JObject payload, 
                                        int clientCount, 
                                        int messageCount,
                                        int messageDelaySeconds,
                                        bool randomize)
        {
            Random rnd = new Random();
            string devicePrefix = "Device" + rnd.Next().ToString() + "-";
            // assuming the string looks like "HostName={hubName}.azure-devices.net;SharedAccessKeyName={keyName};SharedAccessKey={key}"
            string iotHub = iotHubConnectionString.Split(";")[0].Split("=")[1];

            createDevices(iotHubConnectionString, devicePrefix, clientCount, commonKey).ToString();

            for (int sendCounter = 1; sendCounter <= clientCount; sendCounter++)
            {
                await Task.Factory.StartNew(() =>
                {
                    System.Threading.Thread.Sleep(StaticRandomizer.Next(0,3) * 1000);
                    sendMessage(devicePrefix + sendCounter,
                                            commonKey,
                                            iotHub,
                                            messageCount,
                                            payload,
                                            messageDelaySeconds,
                                            randomize).Wait();
                }, TaskCreationOptions.LongRunning);
            }           

            deleteDevice(iotHubConnectionString, devicePrefix, clientCount).Wait();
        }

        private async Task createDevices(string iotHubConnectionString, 
                                        string devicePrefix, 
                                        int clientCount,
                                        string commonKey)
        {
            Microsoft.Azure.Devices.RegistryManager registryManager;
            registryManager = Microsoft.Azure.Devices.RegistryManager.CreateFromConnectionString(iotHubConnectionString);

            for (int deviceCounter = 1; deviceCounter <= clientCount; deviceCounter++)
            {
                Microsoft.Azure.Devices.Device mydevice = new Microsoft.Azure.Devices.Device(devicePrefix + deviceCounter.ToString());
                mydevice.Authentication = new Microsoft.Azure.Devices.AuthenticationMechanism();
                mydevice.Authentication.SymmetricKey.PrimaryKey = commonKey;
                mydevice.Authentication.SymmetricKey.SecondaryKey = commonKey;
                mydevice.Status = Microsoft.Azure.Devices.DeviceStatus.Enabled;
                try
                {
                    await registryManager.AddDeviceAsync(mydevice);
                }
                catch (Exception) { }
            }
            Console.WriteLine("Devices (" + devicePrefix + ") created");
        }

        private static class StaticRandomizer
        {
            private static int _seed = Environment.TickCount;
            private static readonly ThreadLocal<Random> Random =
                new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref _seed)));

            public static int Next(int floor, int cieling)
            {
                return Random.Value.Next(floor, cieling);
            }

            public static double NextDouble()
            {
                return Random.Value.NextDouble();
            }
        }

        private async Task deleteDevice(string iotHubConnectionString,
                                        string devicePrefix,
                                        int clientCount)
        {
            Microsoft.Azure.Devices.RegistryManager registryManager;
            registryManager = Microsoft.Azure.Devices.RegistryManager.CreateFromConnectionString(iotHubConnectionString);

            for (int deviceCounter = 1; deviceCounter <= clientCount; deviceCounter++)
            {
                Microsoft.Azure.Devices.Device mydevice = await registryManager.GetDeviceAsync(devicePrefix + deviceCounter.ToString());

                try
                {
                    await registryManager.RemoveDeviceAsync(mydevice);
                }
                catch (Exception) { }
            }
            Console.WriteLine("Devices (" + devicePrefix + ") deleted");
        }

        private async Task sendMessage(string deviceId, 
                                        string commonKey, 
                                        string iotHub, 
                                        int messageCount, 
                                        JObject payLoad, 
                                        int messageDelaySeconds,
                                        Boolean randomize)
        {
            Microsoft.Azure.Devices.Client.DeviceClient deviceClient;
            string connectionString = "HostName=" + iotHub + ";DeviceId=" + deviceId + ";SharedAccessKey=" + commonKey;
            deviceClient = Microsoft.Azure.Devices.Client.DeviceClient.CreateFromConnectionString(connectionString);

            //Command timeout. Default is 240 seconds
            deviceClient.OperationTimeoutInMilliseconds = 30000; //30 second timeout
            //Set retry policy for sending messages (default is NoRetry)
            deviceClient.SetRetryPolicy(new Microsoft.Azure.Devices.Client.ExponentialBackoff(
                retryCount: int.MaxValue,
                minBackoff: TimeSpan.FromSeconds(1),
                maxBackoff: TimeSpan.FromSeconds(60),
                deltaBackoff: TimeSpan.FromSeconds(2)));

            Random random = new Random();
            int messageCounter = 1;
            while (messageCounter <= messageCount) {

                if (randomize) {
                    int delaySeconds = random.Next(messageDelaySeconds * 1000);
                    payLoad["LOADTEST-delaySeconds"] = delaySeconds / 1000;
                    System.Threading.Thread.Sleep(delaySeconds);
                } else
                {
                    System.Threading.Thread.Sleep(messageDelaySeconds * 1000);
                }

                payLoad["LOADTEST-counter"] = messageCounter;
                payLoad["LOADTEST-generationTimeUTC"] = DateTime.UtcNow.ToString();
                payLoad["LOADTEST-origionatingHost"] = Environment.MachineName;

                Byte[] messageByte = System.Text.Encoding.UTF8.GetBytes(payLoad.ToString());
                Microsoft.Azure.Devices.Client.Message message = new Microsoft.Azure.Devices.Client.Message(messageByte);
                message.ContentEncoding = "utf-8";
                message.ContentType = "application/json";

                try { 
                    await deviceClient.SendEventAsync(message);
                    Console.WriteLine("Device: " + deviceId + " sent msg " + payLoad.ToString());

                    Microsoft.Azure.Devices.Client.Message receivedMessage = await deviceClient.ReceiveAsync(new TimeSpan(0, 0, 1));
                    if (receivedMessage != null)
                    {
                        string jsonMessage = System.Text.Encoding.ASCII.GetString(receivedMessage.GetBytes());
                        Console.WriteLine("Device: " + deviceId + " received msg " + jsonMessage);
                    }
                    messageCounter++;
                }
                catch (Exception) { }
            }
            await deviceClient.CloseAsync();
        }
    }
}

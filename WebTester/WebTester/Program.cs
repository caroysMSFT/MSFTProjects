using System;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace WebTester
{
    class Program
    {
        public static string uri = string.Empty;
        public static int poolsize = 0;
        public static int threads = 1;
        public static int minutes = 1;
        public static int sleepms = 0;
        public static void DoRequest()
        {
            DateTime start = DateTime.Now;
            HttpClient client = new HttpClient();
            //If you're going to mess with things like connection pooling; do it here.
            client.BaseAddress = new Uri(uri);

            while ((DateTime.Now - start).TotalMinutes < minutes)
            {
                if (sleepms > 0) Thread.Sleep(sleepms);
                Task t = GetAsync(client);

                t.Wait();
            }
        }


        static async Task GetAsync(HttpClient httpClient)
        {
            using HttpResponseMessage response = await httpClient.GetAsync("/");
            // We may consider logging this stuff -- but then we get into thread synchronization.  Ick.
            response.EnsureSuccessStatusCode();
        }

        static void Main(string[] args)
        {

            for(int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-threads":
                        threads = Convert.ToInt32(args[i + 1]);
                        break;
                    case "-poolsize":
                        poolsize = Convert.ToInt32(args[i + 1]);
                        break;
                    case "-minutes":
                        minutes = Convert.ToInt32(args[i + 1]);
                        break;
                    case "-sleepms":
                        sleepms = Convert.ToInt32(args[i + 1]);
                        break;
                    case "-uri":
                        uri = args[i + 1];
                        break;
                }
            }

            for(int i = 0; i < threads; i++)
            {
                Thread thread1 = new Thread(DoRequest);
                thread1.Start();
            }
        }
    }
}

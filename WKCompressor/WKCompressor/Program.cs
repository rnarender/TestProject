using NLog;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WKCompressor
{
    class Program
    {
        const byte TerminateSeconds = 5;
        const byte DotsInBetween = 3;
        static Stopwatch StopwatchMain = new Stopwatch();
        private static Logger logger = LogManager.GetCurrentClassLogger();
        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            //StopwatchMain.Start();

            Console.WriteLine("Welcome to WK DB data compression tool. Please choose from the following options");
           // logger.Log(LogLevel.Trace, "Welcome to WK DB data compression tool. Please choose from the following options");
            Console.WriteLine();
            // new Worker().processLast10Records();
            var Worker = new WorkerOne().CompressAllRecords();
            //Console.Read();
        }

        //static void MainOLD(string[] args)
        //{
        //    AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
        //    StopwatchMain.Start();

        //    Console.WriteLine("Welcome to WK DB data compression tool. Please choose from the following options");
        //    Console.WriteLine();
        //    Console.WriteLine("1. Run Data Compression");
        //    Console.WriteLine("2. Run Failed Compressions");
        //    Console.WriteLine("3. Exit");

        //    Console.WriteLine();
        //    Console.Write("Enter your choice: ");
        //    var Input = Console.ReadKey().KeyChar;
        //    Console.WriteLine();
        //    Console.WriteLine();
        //    var Worker = new Worker();
        //    var Result = false;

        //    switch (Input)
        //    {
        //        case '1':
        //            Console.WriteLine("Got your input as 1");
        //            var Stopwatch = new Stopwatch();
        //            Stopwatch.Start();
        //            Result = Worker.CompressAllRecords();
        //            Stopwatch.Stop();
        //            Console.WriteLine($"Total Duration: {Stopwatch.Elapsed.Hours} H {Stopwatch.Elapsed.Minutes} M {Stopwatch.Elapsed.Seconds} S");
        //            //Console.WriteLine("Total time: " + Stopwatch.Elapsed.Minutes + " minutes");
        //            Console.ReadLine();
        //            break;
        //        case '2':
        //            Console.WriteLine("Got your input as 2");
        //            Result = Worker.CompressFailedRecords();
        //            break;
        //        case '3': return;
        //        default:
        //            Console.WriteLine($"No choice was made, application terminates in {TerminateSeconds} seconds");
        //            var Counter = TerminateSeconds;
        //            while (Counter > 0)
        //            {
        //                Console.Write(Counter);
        //                Counter--;
        //                for (int i = 0; i < DotsInBetween; i++)
        //                {
        //                    Thread.Sleep(1000 / DotsInBetween);
        //                    Console.Write(".");
        //                }
        //            }
        //            Console.Write("0" + Environment.NewLine);
        //            break;
        //    }
        //}

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine(e.ExceptionObject.ToString());
            logger.Log(LogLevel.Error, e.ExceptionObject.ToString());
            //StopwatchMain.Stop();
        }
    }
}

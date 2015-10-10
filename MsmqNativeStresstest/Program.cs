using System;
using System.Diagnostics;
using System.Messaging;
using System.Threading;
using System.Threading.Tasks;

namespace MsmqNativeStresstest
{
    class Program
    {
        private static readonly CountdownEvent batchCountdownEvent = new CountdownEvent(batchSize);
        private readonly MessageQueue writeQueue = new MessageQueue(@".\private$\msmqnative", QueueAccessMode.Send);
        private const int batchSize = 1000;
        private long total;
        private readonly byte[] head = new byte[1024];
        private readonly byte[] body = new byte[1024];

        static void Main()
        {
            // 3600 No transaction
            // 2800 Msmq transaction
            // 2000 Transaction scope / DTC

            Console.WriteLine("64bit: {0}", Environment.Is64BitProcess);
            var reader = new MessageProcessor(@".\private$\msmqnative", 4,
                MessageProcessor.MessageProcessorKind.TransactionScope
                //MessageProcessor.MessageProcessorKind.MsmqTransaction
                //MessageProcessor.MessageProcessorKind.NoTransaction
                );

            reader.Open();
            var p = new Program();
            p.Init();
            p.Loop();
        }

        private void Init()
        {
            if (!MessageQueue.Exists(@".\private$\msmqnative"))
                MessageQueue.Create(@".\private$\msmqnative", true);
        }

        private void Loop()
        {

            //q2.Purge();
            var start = Stopwatch.StartNew();
            while (true)
            {
                batchCountdownEvent.Reset();
                var sp = Stopwatch.StartNew();

                Parallel.For(0, batchSize, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, i =>
                {
                    var current = Interlocked.Increment(ref total);
                    var msg = new Message
                    {
                        Recoverable = true,
                        Label = current.ToString(),
                        Body = body,
                        Extension = head,
                    };
                    writeQueue.Send(msg, MessageQueueTransactionType.Single);

                });
                batchCountdownEvent.Wait();
                var elapsed = sp.Elapsed.TotalSeconds;
                Console.WriteLine("{0:N0}msg/s ~{1:N0} +{2:N0} {3}s", batchSize / elapsed, total / start.Elapsed.TotalSeconds, total, start.Elapsed.TotalSeconds);
            }
        }

        internal static void Signal()
        {
            batchCountdownEvent.Signal();
        }
    }
}

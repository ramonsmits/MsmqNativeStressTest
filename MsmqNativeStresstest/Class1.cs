using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace MsmqNativeStresstest
{
    public class MessageProcessor//<TMessage>
    {
        private readonly bool Ambient;
        private long counter;
        private readonly MessageQueue[] Receivers;
        private bool IsClosing;
        //private PeekCompletedEventHandler x;
        public MessageProcessor(string path)
            : this(path, 1, false) { }
        public MessageProcessor(string path, int count, bool ambient)
            : base()
        {
            Ambient = ambient;

            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException("path");
            if (!MessageQueue.Exists(path))
                MessageQueue.Create(path, true);

            this.Receivers = Enumerable.Range(0, (count <= 0) ? 1 : count)
                .Select(i =>
                {
                    var queue = new MessageQueue(path, QueueAccessMode.Receive)
                    {
                        Formatter = new BinaryMessageFormatter()
                    };
                    queue.Purge();
                    queue.MessageReadPropertyFilter.SetAll();
                    return queue;
                })
                .ToArray();
        }
        public void Close()
        {
            this.IsClosing = true;
            this.OnClosing();
            foreach (var queue in Receivers)
            {
                if (Ambient)
                    queue.PeekCompleted -= queue_PeekCompletedAmbient;
                else
                    queue.PeekCompleted -= queue_PeekCompleted;

                queue.Close();
            }
            while (this.IsProcessing)
                Thread.Sleep(100);
            this.IsClosing = this.IsOpen = false;
            this.OnClosed();
        }
        public bool IsOpen { get; private set; }
        protected bool IsProcessing
        {
            get { return Interlocked.Read(ref counter) > 0; }
        }
        protected virtual void OnClosing() { }
        protected virtual void OnClosed() { }
        protected virtual void OnOpening() { }
        protected virtual void OnOpened() { }
        public void Open()
        {
            if (this.IsOpen)
                throw new Exception("This processor is already open.");
            this.OnOpening();
            foreach (var queue in this.Receivers)
            {
                if (Ambient)
                    queue.PeekCompleted += queue_PeekCompletedAmbient;
                else
                    queue.PeekCompleted += queue_PeekCompleted;

                queue.BeginPeek();
            }
            this.IsOpen = true;
            this.OnOpened();
        }
        //protected abstract void Process(TMessage @object);
        private void Handle(Message message)
        {
            Trace.Assert(null != message);
            Interlocked.Increment(ref counter);
            try
            {
                Program.Signal();
            }
            finally
            {
                Interlocked.Decrement(ref counter);
            }
        }
        private void queue_PeekCompleted(object sender, PeekCompletedEventArgs e)
        {
            var queue = (MessageQueue)sender;
            var transaction = new MessageQueueTransaction();
            transaction.Begin();
            try
            {
                // if the queue closes after the transaction begins,
                // but before the call to Receive, then an exception
                // will be thrown and the transaction will be aborted
                // leaving the message to be processed next time
                var msg = queue.Receive(transaction);
                this.Handle(msg);
                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Abort();
                Trace.WriteLine(ex.Message);
            }
            finally
            {
                if (!this.IsClosing)
                    queue.BeginPeek();
            }
        }
        private void queue_PeekCompletedAmbient(object sender, PeekCompletedEventArgs e)
        {
            var queue = (MessageQueue)sender;
            using (var transaction = new TransactionScope())
            {
                try
                {
                    // if the queue closes after the transaction begins,
                    // but before the call to Receive, then an exception
                    // will be thrown and the transaction will be aborted
                    // leaving the message to be processed next time
                    var msg = queue.Receive(MessageQueueTransactionType.Automatic);
                    Handle(msg);
                    transaction.Complete();
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(ex.Message);
                }
                finally
                {
                    if (!this.IsClosing)
                        queue.BeginPeek();
                }
            }
        }
    }

}

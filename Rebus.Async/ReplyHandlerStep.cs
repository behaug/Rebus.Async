using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Threading;
#pragma warning disable 1998

namespace Rebus.Async
{
    class ReplyHandlerStep : IIncomingStep, IInitializable, IDisposable
    {
        readonly ConcurrentDictionary<string, TimedMessage> _messages;
        readonly TimeSpan _replyMaxAge;
        readonly IAsyncTask _cleanupTask;
        readonly ILog _log;

        public ReplyHandlerStep(ConcurrentDictionary<string, TimedMessage> messages, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, TimeSpan replyMaxAge)
        {
            _messages = messages;
            _replyMaxAge = replyMaxAge;
            _log = rebusLoggerFactory.GetCurrentClassLogger();
            _cleanupTask = asyncTaskFactory.Create("CleanupAbandonedRepliesTask", CleanupAbandonedReplies);
        }

        public const string SpecialCorrelationIdPrefix = "request-reply";

        public const string SpecialRequestTag = "request-tag";

        public async Task Process(IncomingStepContext context, Func<Task> next)
        {
            var message = context.Load<Message>();

            string messageIntent;
            if (!message.Headers.TryGetValue(Headers.Intent, out messageIntent))
                messageIntent = "";

            string correlationId;

            var hasCorrelationId = message.Headers.TryGetValue(Headers.CorrelationId, out correlationId);
            if (hasCorrelationId)
            {
                var isRequestReplyCorrelationId = correlationId.StartsWith(SpecialCorrelationIdPrefix);
                if (isRequestReplyCorrelationId)
                {
                    var isRequest = message.Headers.ContainsKey(SpecialRequestTag);
                    if (!isRequest)
                    {
                        // it's a reply
                        string replyKey = $"{correlationId}:{message.Body.GetType().FullName}";
                        _messages[replyKey] = new TimedMessage(message);

                        if (messageIntent == "p2p")
                            return;
                    }
                }
            }

            await next();
        }

        public void Initialize()
        {
            _cleanupTask.Start();
        }

        public void Dispose()
        {
            _cleanupTask.Dispose();
        }

        async Task CleanupAbandonedReplies()
        {
            var messageList = _messages.ToList();

            var itemsToRemove = messageList
                .Where(m => m.Value.Age > _replyMaxAge)
                .ToList();

            if (!itemsToRemove.Any()) return;

            _log.Info("Found {0} reply messages whose age exceeded {1} - removing them now!", itemsToRemove.Count, _replyMaxAge);

            foreach (var itemToRemove in itemsToRemove)
            {
                _log.Debug($"Removing abandoned reply of type {itemToRemove.Value.Message.Body.GetType().FullName}");
                TimedMessage temp;
                _messages.TryRemove(itemToRemove.Key, out temp);
            }
        }
    }
}
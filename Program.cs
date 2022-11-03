// See https://aka.ms/new-console-template for more information
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

var queueName = "lock-repro";
int numberOfMessages =  10;
var workDuration = TimeSpan.FromSeconds(45);

var adminClient = new ServiceBusAdministrationClient(Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString"));
// make sure we have a clean slate
if(await adminClient.QueueExistsAsync(queueName)) 
{
    await adminClient.DeleteQueueAsync(queueName);
}
// create with a short lock duration to make sure things don't take ages
await adminClient.CreateQueueAsync(new CreateQueueOptions(queueName) {
    LockDuration = TimeSpan.FromSeconds(30)
});

await using var serviceBusClient = new ServiceBusClient(Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString"));
await using var sender = serviceBusClient.CreateSender(queueName);
await using var processor = serviceBusClient.CreateProcessor(queueName, new ServiceBusProcessorOptions {
    AutoCompleteMessages = false,
    MaxAutoLockRenewalDuration = workDuration.Add(TimeSpan.FromSeconds(15)),
    // Prefetch more than we handle
    PrefetchCount = numberOfMessages,
    // Make sure we handle less than prefetched
    MaxConcurrentCalls = numberOfMessages / 2
});

var messages = new List<ServiceBusMessage>(numberOfMessages);
for (int i = 0; i < numberOfMessages; i++)
{
    messages.Add(new ServiceBusMessage(i.ToString()));
}
await sender.SendMessagesAsync(messages);
await Console.Error.WriteLineAsync($"{DateTimeOffset.UtcNow}: {messages.Count} messages sent.");

processor.ProcessErrorAsync += async args => 
{
    await Console.Error.WriteLineAsync(args.Exception.Message);
};
processor.ProcessMessageAsync += async args =>
{
    var message = args.Message;
    var body = message.Body.ToString();
    if(message.LockedUntil <= DateTimeOffset.UtcNow) 
    {
        await Console.Error.WriteLineAsync($"{DateTimeOffset.UtcNow}: Skipping message {body}. LockUntil was {message.LockedUntil}");
        // Uncomment the line to make the message appear
        await args.AbandonMessageAsync(message);    
        return;
    }
    await Console.Error.WriteLineAsync($"{DateTimeOffset.UtcNow}: Handling message {body}.");
    await Task.Delay(workDuration);
    await args.CompleteMessageAsync(message);
    await Console.Error.WriteLineAsync($"{DateTimeOffset.UtcNow}: Done {body}.");
};
await processor.StartProcessingAsync();
Console.ReadLine();
await processor.StopProcessingAsync();
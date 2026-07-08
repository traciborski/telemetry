
// public class LoggingMetrics
// {
//     private readonly Counter<long> _logEventsCounter;
//     private readonly Histogram<double> _logProcessingDuration;
// public LoggingMetrics(IMeterFactory meterFactory)
//     {
//         var meter = meterFactory.Create("MyApp.Logging");
//         _logEventsCounter = meter.CreateCounter<long>("log_events_total");
//         _logProcessingDuration = meter.CreateHistogram<double>("log_processing_duration_ms");
//     }
//     public void RecordLogEvent(LogEventLevel level)
//     {
//         _logEventsCounter.Add(1, new KeyValuePair<string, object>("level", level.ToString()));
//     }
// }
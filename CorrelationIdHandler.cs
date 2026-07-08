using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

public class CorrelationIdHandler : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var activity = Activity.Current;
        if (activity != null)
        {
            request.Headers.Add("X-Correlation-ID", activity.TraceId.ToString());
        }
        return await base.SendAsync(request, cancellationToken);
    }
}
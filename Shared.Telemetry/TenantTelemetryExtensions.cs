using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;

namespace Shared.Telemetry;

public static class TenantTelemetryExtensions
{
    public const string TenantIdAttributeName = "tenant.id";
    public const string TenantIdHeaderName = "tenant-id";

    public static IApplicationBuilder UseTenantTelemetry(this IApplicationBuilder app, params string[] excludedPaths)
    {
        var pathsToSkip = excludedPaths is { Length: > 0 } ? excludedPaths : ["/health"];

        return app.Use(async (context, next) =>
        {
            foreach (var excludedPath in pathsToSkip)
            {
                if (context.Request.Path.Equals(excludedPath, StringComparison.OrdinalIgnoreCase))
                {
                    await next();
                    return;
                }
            }

            var tenantId = RequireTenantIdFromHeaders(context.Request.Headers);
            EnsureTenantMatchesTrace(tenantId);

            var activity = Activity.Current;
            activity?.SetTag(TenantIdAttributeName, tenantId);
            activity?.SetBaggage(TenantIdAttributeName, tenantId);

            var logger = context.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("TenantTelemetry");
            using (logger.BeginScope(new Dictionary<string, object> { [TenantIdAttributeName] = tenantId }))
            {
                await next();
            }
        });
    }

    public static IHttpClientBuilder AddTenantHeaderPropagation(this IHttpClientBuilder builder)
        => builder.AddHttpMessageHandler(() => new TenantHeaderPropagationHandler());

    private static string? GetCurrentTenantId()
    {
        var activity = Activity.Current;
        return activity?.GetBaggageItem(TenantIdAttributeName) ?? activity?.GetTagItem(TenantIdAttributeName)?.ToString();
    }

    private static string RequireCurrentTenantId()
        => GetCurrentTenantId() ?? throw new InvalidOperationException($"Missing required tenant header '{TenantIdHeaderName}'.");

    private static string? GetTenantIdFromHeaders(IHeaderDictionary headers)
        => GetTenantIdFromHeaders(headers.TryGetValue(TenantIdHeaderName, out var tenantIdValue) ? tenantIdValue : default);

    private static string RequireTenantIdFromHeaders(IHeaderDictionary headers)
        => GetTenantIdFromHeaders(headers) ?? throw new InvalidOperationException($"Missing required tenant header '{TenantIdHeaderName}'.");

    private static void EnsureTenantMatchesTrace(string tenantId)
    {
        var currentTenantId = GetCurrentTenantId();
        if (!string.IsNullOrWhiteSpace(currentTenantId) && !string.Equals(currentTenantId, tenantId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Tenant mismatch in trace: current tenant '{currentTenantId}' does not match incoming tenant '{tenantId}'.");
        }
    }

    private static string? GetTenantIdFromHeaders(StringValues headerValue)
    {
        var candidate = headerValue.Count > 0 ? headerValue[0] : null;
        return string.IsNullOrWhiteSpace(candidate) ? null : candidate;
    }

    private sealed class TenantHeaderPropagationHandler : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var tenantId = RequireCurrentTenantId();
            request.Headers.Remove(TenantIdHeaderName);
            request.Headers.TryAddWithoutValidation(TenantIdHeaderName, tenantId);
            return base.SendAsync(request, cancellationToken);
        }
    }
}

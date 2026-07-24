using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Shared.Outbox;

public class HeadersConverter : ValueConverter<Dictionary<string, string>, string>
{
    public HeadersConverter() : base(t => JsonSerializer.Serialize(t, new JsonSerializerOptions()), t => JsonSerializer.Deserialize<Dictionary<string, string>>(t, new JsonSerializerOptions()))
    {
    }
}
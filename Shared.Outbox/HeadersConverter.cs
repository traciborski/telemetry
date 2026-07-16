using System.Linq.Expressions;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Shared.Outbox;

public class HeadersConverter : ValueConverter<Dictionary<string, string>, string>
{
    public HeadersConverter()
        : base((Dictionary<string, string> t) => JsonSerializer.Serialize(t, new JsonSerializerOptions()), (Expression<Func<string, Dictionary<string, string>>>)((string t) => JsonSerializer.Deserialize<Dictionary<string, string>>(t, new JsonSerializerOptions())), (ConverterMappingHints?)null)
    {
    }
}
using Microsoft.Extensions.Logging;

namespace IntegrationTests;

public class XUnitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _output;

    public XUnitLoggerProvider(ITestOutputHelper output)
    {
        _output = output;
    }

    public ILogger CreateLogger(string categoryName) =>
        new XUnitLogger(_output, categoryName);

    public void Dispose() { }

    private class XUnitLogger : ILogger
    {
        private readonly ITestOutputHelper _output;
        private readonly string _category;

        public XUnitLogger(ITestOutputHelper output, string category)
        {
            _output = output;
            _category = category;
        }

        public IDisposable BeginScope<TState>(TState state) => default!;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;

            try
            {
                var message = formatter(state, exception);
                _output.WriteLine($"{DateTime.Now:HH:mm:ss.fff} [{logLevel}] {_category}: {message}");
                if (exception != null)
                {
                    _output.WriteLine(exception.ToString());
                }
            }
            catch
            {
                // Ne jamais throw vers xUnit
            }
        }
    }
}
using System;

// Minimal Windows helper for Spark local-mode development.
// Hadoop invokes `winutils chmod` after downloading connector JARs. NTFS
// permissions are already handled by Windows, so that operation can safely be
// treated as a no-op for this single-user local pipeline.
internal static class WinUtils
{
    private static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Usage: winutils <chmod|chown> [arguments]");
            return 1;
        }

        string command = args[0].ToLowerInvariant();
        if (command == "chmod" || command == "chown")
        {
            return 0;
        }

        Console.Error.WriteLine("Unsupported winutils command: " + args[0]);
        return 1;
    }
}

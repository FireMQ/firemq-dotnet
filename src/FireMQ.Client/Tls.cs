using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace FireMQ;

/// <summary>Wraps a connection in TLS for <c>fires://</c>.</summary>
internal static class Tls
{
    public static async Task<SslStream> AuthenticateAsync(Stream inner, ConnectUrl url, ConnectOptions options, CancellationToken cancellationToken)
    {
        X509Certificate2Collection? roots = string.IsNullOrWhiteSpace(options.TlsCaPem) ? null : LoadPem(options.TlsCaPem!);
        bool insecure = options.TlsInsecure;
        var ssl = new SslStream(inner, leaveInnerStreamOpen: false);
        var auth = new SslClientAuthenticationOptions
        {
            TargetHost = string.IsNullOrEmpty(options.TlsServerName) ? url.Host : options.TlsServerName,
            EnabledSslProtocols = SslProtocols.None,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            RemoteCertificateValidationCallback = (_, cert, chain, errors) => Validate(cert, chain, errors, roots, insecure),
        };
        try
        {
            await ssl.AuthenticateAsClientAsync(auth, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ssl.Dispose();
            throw;
        }
        return ssl;
    }

    private static bool Validate(X509Certificate? cert, X509Chain? chain, SslPolicyErrors errors, X509Certificate2Collection? roots, bool insecure)
    {
        if (insecure || errors == SslPolicyErrors.None)
        {
            return true;
        }
        if (roots is null || cert is null || (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != SslPolicyErrors.None)
        {
            return false;
        }

        // The system store does not trust the chain; check it against the configured CAs.
        using var custom = new X509Chain();
        custom.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        if (chain is not null)
        {
            foreach (var element in chain.ChainElements)
            {
                custom.ChainPolicy.ExtraStore.Add(element.Certificate);
            }
        }
        var leaf = cert as X509Certificate2 ?? new X509Certificate2(cert);
#if NET5_0_OR_GREATER
        custom.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        custom.ChainPolicy.CustomTrustStore.AddRange(roots);
        return custom.Build(leaf);
#else
        custom.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
        custom.ChainPolicy.ExtraStore.AddRange(roots);
        if (!custom.Build(leaf))
        {
            return false;
        }
        var root = custom.ChainElements[custom.ChainElements.Count - 1].Certificate;
        foreach (var trusted in roots)
        {
            if (trusted.RawData.AsSpan().SequenceEqual(root.RawData))
            {
                return true;
            }
        }
        return false;
#endif
    }

    /// <summary>Loads certificates from PEM text, or from a PEM file when given a path.</summary>
    internal static X509Certificate2Collection LoadPem(string pemOrPath)
    {
        const string Begin = "-----BEGIN CERTIFICATE-----";
        const string End = "-----END CERTIFICATE-----";
        string pem = pemOrPath;
        if (!pem.Contains(Begin) && File.Exists(pem))
        {
            pem = File.ReadAllText(pem);
        }
        var certs = new X509Certificate2Collection();
        int pos = 0;
        while ((pos = pem.IndexOf(Begin, pos, StringComparison.Ordinal)) >= 0)
        {
            int start = pos + Begin.Length;
            int stop = pem.IndexOf(End, start, StringComparison.Ordinal);
            if (stop < 0)
            {
                break;
            }
            string base64 = new(pem.Substring(start, stop - start).Where(c => !char.IsWhiteSpace(c)).ToArray());
            certs.Add(new X509Certificate2(Convert.FromBase64String(base64)));
            pos = stop + End.Length;
        }
        if (certs.Count == 0)
        {
            throw new ArgumentException("TlsCaPem holds no PEM certificate", nameof(ConnectOptions.TlsCaPem));
        }
        return certs;
    }
}

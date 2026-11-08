using System.Buffers;

namespace FireMQ.Protocol;

/// <summary>A growable byte buffer that MessagePack writers can write into.</summary>
internal sealed class ByteBuffer : IBufferWriter<byte>
{
    private byte[] _buffer;
    private int _length;

    public ByteBuffer(int capacity = 256)
    {
        _buffer = new byte[Math.Max(capacity, 16)];
    }

    public int Length => _length;

    public byte this[int index]
    {
        get => _buffer[index];
        set => _buffer[index] = value;
    }

    public Span<byte> WrittenSpan => _buffer.AsSpan(0, _length);

    public void Advance(int count)
    {
        if (count < 0 || _length + count > _buffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }
        _length += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsMemory(_length);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsSpan(_length);
    }

    public void Write(ReadOnlySpan<byte> data)
    {
        Ensure(data.Length);
        data.CopyTo(_buffer.AsSpan(_length));
        _length += data.Length;
    }

    /// <summary>Returns the written bytes. The array is exactly <see cref="Length"/> bytes long.</summary>
    public byte[] ToArray()
    {
        if (_length == _buffer.Length)
        {
            return _buffer;
        }
        return _buffer.AsSpan(0, _length).ToArray();
    }

    private void Ensure(int sizeHint)
    {
        if (sizeHint < 1)
        {
            sizeHint = 1;
        }
        if (_buffer.Length - _length >= sizeHint)
        {
            return;
        }
        long wanted = Math.Max((long)_buffer.Length * 2, (long)_length + sizeHint);
        if (wanted > int.MaxValue)
        {
            wanted = int.MaxValue;
        }
        Array.Resize(ref _buffer, (int)wanted);
    }
}

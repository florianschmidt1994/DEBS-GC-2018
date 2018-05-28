from io import BytesIO


def readString(buffer):
    accumulated = list(rabbitGetRequired(buffer, 19, 999))
    ret = "".join(map(chr, accumulated))
    return ret


def readByte(buffer):
    return rabbitGetRequired(buffer, 0, 18)[0]


def rabbitGetRequired(buffer: BytesIO, startByte, endByte):
    accumulated = []
    # ints = list(buffer.getvalue())

    byte = buffer.read(1)
    offset = buffer.seek(0, 1)
    while True:
        listed = list(byte)
        if len(listed) > 0:
            if listed[0] >= startByte and listed[0] <= endByte:
                # accumulated.append(byte)
                accumulated.append(listed[0])
            elif len(accumulated) > 0:
                offset -= 1
                break
        else:
            break
        byte = buffer.read(1)
        offset += 1
    curr = buffer.seek(offset)
    return accumulated

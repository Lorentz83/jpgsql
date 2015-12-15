package edu.purdue.jpgs.type;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public abstract class BaseInt {

    private final int _value;
    final int _bits;

    public BaseInt(int bits) {
        _value = 0;
        _bits = bits;
    }

    public BaseInt(int bits, int value) {
        if (bits != 8 && bits != 16 && bits != 32) {
            throw new IllegalArgumentException("BaseInt can hold only 8 16 32 bits integers");
        }
        _bits = bits;
        _value = value;
    }

//    explicit Int(const std::array<char, bits/8>& val) {
//        unsigned int res = 0;
//        for (int pos = 0; pos < bits / 8; pos++) {
//            res = (res << 8) + ((int) val.at(pos) & 0x000000ff);
//        }
//        value = res;
//    }
    public char[] toBytes() {
        int bytes = _bits / 8;
        char[] ret = new char[bytes];
        int bitshift = _bits - 8;
        for (int pos = 0; pos < bytes; pos++) {
            ret[pos] = (char) (_value >>> bitshift);
            bitshift -= 8;
        }
        return ret;
    }

    public int val() {
        return _value;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(_value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BaseInt other = (BaseInt) obj;
        return _value == other._value && _bits == other._bits;
    }

}

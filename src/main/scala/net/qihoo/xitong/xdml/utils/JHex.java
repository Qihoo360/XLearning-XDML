package net.qihoo.xitong.xdml.utils;

import java.math.BigInteger;
/**
 * JHex工具类
 *
 */
public final class JHex {
	private JHex() {
	};

	private static final char[] chs = { 
			'0', '1', '2', '3', '4', '5', '6', '7', 
			'8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	// ******************************byte[]数组的转化*********************************//
	/*
	 * int to byte[]
	 */
	public static byte[] toByteArray(int number) {
		byte[] bytes = new byte[4];
		for (int i = 0; i < 4; i++) {
			bytes[i] = (byte) (number >>> (24 - i << 3));
		}
		return bytes;
	}
	/*
	 * int[] to byte[]
	 */
	public static byte[] toByteArray(int[] numbers) {
		return toByteArray(numbers, 0, numbers.length);
	}

	/*
	 * int[] to byte[],指定开始和结束位置
	 */
	public static byte[] toByteArray(int[] numbers, int from, int to) {
		if (numbers == null)
			throw new IllegalArgumentException("integer array is null!");
		if (from > numbers.length || to > numbers.length || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		int len = to - from;
		byte[] bytes = new byte[len << 2];
		for (int i = 0; i < len; i++) {
			for (int j = 0; j < 4; j++) {
				bytes[j + i << 2] = (byte) (numbers[i + from] >>> (24 - j << 3));
			}
		}
		return bytes;
	}

	/*
	 * float to byte[]
	 */
	public static byte[] toByteArray(float number) {
		int floatBits = Float.floatToIntBits(number);
		return toByteArray(floatBits);
	}

	/*
	 * float[] to byte[]
	 */
	public static byte[] toByteArray(float[] numbers) {
		return toByteArray(numbers, 0, numbers.length);
	}

	/*
	 * float[] to byte[],指定起始和结束位置。
	 */
	public static byte[] toByteArray(float[] numbers, int from, int to) {
		if (numbers == null)
			throw new IllegalArgumentException("integer array is null!");
		if (from > numbers.length || to > numbers.length || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		int len = to - from;
		byte[] bytes = new byte[len << 2];
		for (int i = 0; i < len; i++) {
			for (int j = 0; j < 4; j++) {
				bytes[j + i << 2] = (byte) (Float.floatToIntBits(numbers[i + from]) >>> (24 - j << 3));
			}
		}
		return bytes;
	}

	/*
	 * double to byte[]
	 */
	public static byte[] toByteArray(double number) {
		return toByteArray(new double[] {number},0,1);
	}

	/*
	 * double[] to byte[]
	 */
	public static byte[] toByteArray(double[] numbers) {
		return toByteArray(numbers, 0, numbers.length);
	}

	/*
	 * double[] to byte[]，指定起始和结束位置。
	 */
	public static byte[] toByteArray(double[] numbers, int from, int to) {
		if (numbers == null)
			throw new IllegalArgumentException("integer array is null!");
		if (from > numbers.length || to > numbers.length || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		int len = to - from;
		byte[] bytes = new byte[len << 3];
		for (int i = 0; i < len; i++) {
			for (int j = 0; j < 8; j++) {
				bytes[j + i << 3] = (byte) (Double.doubleToLongBits(numbers[i + from]) >>> (56 - j << 3));
			}
		}
		return bytes;
	}

	/*
	 * byte[] to int
	 */
	public static int toInt(byte[] arr) {
		if (arr == null)
			throw new IllegalArgumentException("byte array is null!");
		if (arr.length > 4)
			throw new IllegalArgumentException("byte array's length is too large!");
		return toInt(arr, 0, arr.length);
	}

	/*
	 * byte[] to int,指定的连续字节
	 */
	public static int toInt(byte[] arr, int from, int to) {
		if (arr == null)
			throw new IllegalArgumentException("byte array is null!");
		if (from > arr.length || to > arr.length || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		if (to - from > 4)
			throw new IllegalArgumentException(
					"byte array's length between [from:" + from + "] and [to:" + to + "]is too large!");
		int result = 0;
		for (int i = from; i < to; i++) {
			result <<= 8;
			result |= (arr[i] & 0xff);
		}
		return result;
	}
	
	/*
	 * binary,hex,octal String to int
	 */
	public static int toInt(String number) {
		return toInt(number,10);
	}


	/*
	 * binary,hex,octal String to int
	 */
	public static int toInt(String number, int radix) {
		BigInteger big = new BigInteger(number, radix);
		return big.intValue();
	}

	/*
	 * byte[] to int[]
	 */
	public static int[] toIntArray(byte[] arr, int byteNumPerInt) {
		return toIntArray(arr, byteNumPerInt, 0, arr.length);
	}

	/*
	 * byte[] to int[],指定的连续字节
	 */
	public static int[] toIntArray(byte[] arr, int byteNumPerInt, int from, int to) {
		if (arr == null)
			throw new IllegalArgumentException("byte array is null!");
		if (byteNumPerInt > 4)
			throw new IllegalArgumentException("parameter [byteNumPerInt:" + byteNumPerInt + "] is too large!");
		if (from > arr.length || to > arr.length || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		int len = to - from;
		if (len % byteNumPerInt != 0)
			throw new IllegalArgumentException("byte array'length between [from:" + from + "] and [to:" + to
					+ "] should be integral multiple of parameter [byteNumPerInt:" + byteNumPerInt + "]!");
		int[] result = new int[len / byteNumPerInt];
		for (int i = 0; i < result.length; i++) {
			result[i] = toInt(arr, from + i * byteNumPerInt, from + (i + 1) * byteNumPerInt);
		}
		return result;
	}

	/*
	 * byte[] to float
	 */
	public static float toFloat(byte[] arr) {
		if (arr == null)
			throw new IllegalArgumentException("byte array is null!");
		if (arr.length != 4)
			throw new IllegalArgumentException("byte array's length is not equals 4!");
		int fInt = toInt(arr);
		return Float.intBitsToFloat(fInt);
	}
	/*
	 * string to float
	 */
	public static float toFloat(String number) {
		return Float.parseFloat(number);
	}
	
	/*
	 * byte[] to float[]
	 */
	public static float[] toFloatArray(byte[] arr) {
		return toFloatArray(arr, 0, arr.length);
	}

	/*
	 * byte[] to float[],指定起始和终点位置
	 */
	public static float[] toFloatArray(byte[] arr, int from, int to) {
		if (arr == null)
			throw new IllegalArgumentException("byte array is null!");
		if (from > arr.length || to > arr.length || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		int len = to - from;
		if (len % 4 != 0)
			throw new IllegalArgumentException("byte array's [length:" + len + "] should be integral multiple of 4!");
		float[] result = new float[len / 4];
		int intBits = 0;
		for (int i = 0; i < result.length; i++) {
			intBits = toInt(arr, from + i << 2, from + (i + 1) << 2);
			result[i] = Float.intBitsToFloat(intBits);
		}
		return result;
	}

	// ******************************无符号数的转化*********************************//
	/*
	 * int to unsigned
	 */
	public static long toUnsigned(int number) {
		return ((long) number) & 0xffffffffL;
	}

	/*
	 * short to unsigned
	 */
	public static int toUnsigned(short number) {
		return ((int) number) & 0xffff;
	}

	/*
	 * byte to unsigned
	 */
	public static short toUnsigned(byte number) {
		return (short) (number & 0xff);
	}

	// ******************************进制转化*********************************//
	/*
	 * 任意进制间的转换
	 */
	public static String transRadix(String number, int fromRadix, int toRadix) {
		if (number == null || number.equals(""))
			throw new IllegalArgumentException("parameter [number:" + number + "] is illegal!");
		if (fromRadix > 16 || fromRadix < 2)
			throw new IllegalArgumentException("parameter [fromRadix:" + fromRadix + "] should between 2 and 16!");
		if (toRadix > 16 || toRadix < 2)
			throw new IllegalArgumentException("parameter [toRadix:" + toRadix + "] should between 2 and 16!");
		int num = Integer.parseInt(number, fromRadix);
		StringBuilder sb = new StringBuilder();
		while (num != 0) {
			sb.append(chs[num % toRadix]);
			num = num / toRadix;
		}
		return sb.reverse().toString();
	}

	/*
	 * to integer's hex string
	 */
	public static String toHexString(int number) {
		return Integer.toHexString(number);
	}

	/*
	 * to integer's hex string,指定最短字符串长度，不足用0补齐.
	 */
	public static String toHexString(int number, int minHexStringLength) {
		int len = toHexString(number).length();
		if (minHexStringLength < len)
			throw new IllegalArgumentException(
					"parameter [minHexStringLength:" + minHexStringLength + "] is illegal:" + minHexStringLength);
		String format = "%0" + minHexStringLength + "x";
		return String.format(format, number);
	}

	/*
	 * to integer's binary string
	 */
	public static String toBinaryString(int number) {
		return Integer.toBinaryString(number);
	}

	/*
	 * to integer's binary string，指定最短字符串长度，不足用0补齐.
	 */
	public static String toBinaryString(int number, int minBinaryStringLength) {
		int len = toBinaryString(number).length();
		if (minBinaryStringLength < len)
			throw new IllegalArgumentException(
					"parameter [minBinaryStringLength:" + minBinaryStringLength + "] is illegal!");
		int lackZeroNumber = minBinaryStringLength - len;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < lackZeroNumber; i++)
			sb.append('0');
		return sb.toString() + Integer.toBinaryString(number);
	}

	// ******************************提取二进制中的某些位*********************************//
	/*
	 * 抽取int中的某些二进制位
	 */
	public static String extractBits(int number, int from, int to) {
		if (from > 31 || to > 31 || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		String bits = toBinaryString(number, 32);
		return bits.substring(32 - to, 32 - from);
	}

	/*
	 * 截取整型数中某些连续的二进制位并获得其值。
	 */
	public static int getBitsValue(int number, int from, int to) {
		if (from > 31 || to > 31 || from < 0 || to < 0)
			throw new IllegalArgumentException("parameter [from:" + from + "] or [to:" + to + "] is illegal!");
		if (from >= to)
			throw new IllegalArgumentException(
					"parameter [from:" + from + "] should be smaller than parameter [to:" + to + "]!");
		String bits = toBinaryString(number, 32);
		bits = bits.substring(32 - to, 32 - from);
		BigInteger big = new BigInteger(bits, 2);
		return big.intValue();
	}

	/*
	 * 获取整形中某一位的二进制值是1还是0并返回
	 */
	public static int bitValueAt(int number, int bitAt) {
		if (bitAt > 31 || bitAt < 0)
			throw new IllegalArgumentException("parameter [bitAt:" + bitAt + "] should between 0 and 31!");
		return (number >>> bitAt) & 0x01;
	}
}

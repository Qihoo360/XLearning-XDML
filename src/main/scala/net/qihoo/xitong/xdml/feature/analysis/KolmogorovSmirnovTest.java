package net.qihoo.xitong.xdml.feature.analysis;

import org.apache.commons.math3.distribution.EnumeratedRealDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.exception.*;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.fraction.BigFraction;
import org.apache.commons.math3.fraction.BigFractionField;
import org.apache.commons.math3.fraction.FractionConversionException;
import org.apache.commons.math3.linear.Array2DRowFieldMatrix;
import org.apache.commons.math3.linear.FieldMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.MathArrays;

import java.util.Arrays;
import java.util.HashSet;

//WARNING: Copy from commons.math3.KolmogorovSmirnovTest, just want to avoid version problem when calculate KS and import commons.math3 from jar in spark cluster

public class KolmogorovSmirnovTest {
    protected static final int MAXIMUM_PARTIAL_SUM_COUNT = 100000;
    protected static final double KS_SUM_CAUCHY_CRITERION = 1.0E-20D;
    protected static final double PG_SUM_RELATIVE_ERROR = 1.0E-10D;
    /** @deprecated */
    @Deprecated
    protected static final int SMALL_SAMPLE_PRODUCT = 200;
    protected static final int LARGE_SAMPLE_PRODUCT = 10000;
    /** @deprecated */
    @Deprecated
    protected static final int MONTE_CARLO_ITERATIONS = 1000000;
    private final RandomGenerator rng;

    public KolmogorovSmirnovTest() {
        this.rng = new Well19937c();
    }

    /** @deprecated */
    @Deprecated
    public KolmogorovSmirnovTest(RandomGenerator rng) {
        this.rng = rng;
    }

    public double kolmogorovSmirnovTest(RealDistribution distribution, double[] data, boolean exact) {
        return 1.0D - this.cdf(this.kolmogorovSmirnovStatistic(distribution, data), data.length, exact);
    }

    public double kolmogorovSmirnovStatistic(RealDistribution distribution, double[] data) {
        this.checkArray(data);
        int n = data.length;
        double nd = (double)n;
        double[] dataCopy = new double[n];
        System.arraycopy(data, 0, dataCopy, 0, n);
        Arrays.sort(dataCopy);
        double d = 0.0D;

        for(int i = 1; i <= n; ++i) {
            double yi = distribution.cumulativeProbability(dataCopy[i - 1]);
            double currD = FastMath.max(yi - (double)(i - 1) / nd, (double)i / nd - yi);
            if(currD > d) {
                d = currD;
            }
        }

        return d;
    }

    public double kolmogorovSmirnovTest(double[] x, double[] y, boolean strict) {
        long lengthProduct = (long)x.length * (long)y.length;
        Object xa = null;
        Object ya = null;
        double[] xa1;
        double[] ya1;
        if(lengthProduct < 10000L && hasTies(x, y)) {
            xa1 = MathArrays.copyOf(x);
            ya1 = MathArrays.copyOf(y);
            fixTies(xa1, ya1);
        } else {
            xa1 = x;
            ya1 = y;
        }

        return lengthProduct < 10000L?this.exactP(this.kolmogorovSmirnovStatistic(xa1, ya1), x.length, y.length, strict):this.approximateP(this.kolmogorovSmirnovStatistic(x, y), x.length, y.length);
    }

    public double kolmogorovSmirnovTest(double[] x, double[] y) {
        return this.kolmogorovSmirnovTest(x, y, true);
    }

    public double kolmogorovSmirnovStatistic(double[] x, double[] y) {
        return (double)this.integralKolmogorovSmirnovStatistic(x, y) / (double)((long)x.length * (long)y.length);
    }

    private long integralKolmogorovSmirnovStatistic(double[] x, double[] y) {
        this.checkArray(x);
        this.checkArray(y);
        double[] sx = MathArrays.copyOf(x);
        double[] sy = MathArrays.copyOf(y);
        Arrays.sort(sx);
        Arrays.sort(sy);
        int n = sx.length;
        int m = sy.length;
        int rankX = 0;
        int rankY = 0;
        long curD = 0L;
        long supD = 0L;

        do {
            double z;
            for(z = Double.compare(sx[rankX], sy[rankY]) <= 0?sx[rankX]:sy[rankY]; rankX < n && Double.compare(sx[rankX], z) == 0; curD += (long)m) {
                ++rankX;
            }

            while(rankY < m && Double.compare(sy[rankY], z) == 0) {
                ++rankY;
                curD -= (long)n;
            }

            if(curD > supD) {
                supD = curD;
            } else if(-curD > supD) {
                supD = -curD;
            }
        } while(rankX < n && rankY < m);

        return supD;
    }

    public double kolmogorovSmirnovTest(RealDistribution distribution, double[] data) {
        return this.kolmogorovSmirnovTest(distribution, data, false);
    }

    public boolean kolmogorovSmirnovTest(RealDistribution distribution, double[] data, double alpha) {
        if(alpha > 0.0D && alpha <= 0.5D) {
            return this.kolmogorovSmirnovTest(distribution, data) < alpha;
        } else {
            throw new OutOfRangeException(LocalizedFormats.OUT_OF_BOUND_SIGNIFICANCE_LEVEL, Double.valueOf(alpha), Integer.valueOf(0), Double.valueOf(0.5D));
        }
    }

    public double bootstrap(double[] x, double[] y, int iterations, boolean strict) {
        int xLength = x.length;
        int yLength = y.length;
        double[] combined = new double[xLength + yLength];
        System.arraycopy(x, 0, combined, 0, xLength);
        System.arraycopy(y, 0, combined, xLength, yLength);
        EnumeratedRealDistribution dist = new EnumeratedRealDistribution(this.rng, combined);
        long d = this.integralKolmogorovSmirnovStatistic(x, y);
        int greaterCount = 0;
        int equalCount = 0;

        for(int i = 0; i < iterations; ++i) {
            double[] curX = dist.sample(xLength);
            double[] curY = dist.sample(yLength);
            long curD = this.integralKolmogorovSmirnovStatistic(curX, curY);
            if(curD > d) {
                ++greaterCount;
            } else if(curD == d) {
                ++equalCount;
            }
        }

        return strict?(double)greaterCount / (double)iterations:(double)(greaterCount + equalCount) / (double)iterations;
    }

    public double bootstrap(double[] x, double[] y, int iterations) {
        return this.bootstrap(x, y, iterations, true);
    }

    public double cdf(double d, int n) throws MathArithmeticException {
        return this.cdf(d, n, false);
    }

    public double cdfExact(double d, int n) throws MathArithmeticException {
        return this.cdf(d, n, true);
    }

    public double cdf(double d, int n, boolean exact) throws MathArithmeticException {
        double ninv = 1.0D / (double)n;
        double ninvhalf = 0.5D * ninv;
        if(d <= ninvhalf) {
            return 0.0D;
        } else if(ninvhalf < d && d <= ninv) {
            double res = 1.0D;
            double f = 2.0D * d - ninv;

            for(int i = 1; i <= n; ++i) {
                res *= (double)i * f;
            }

            return res;
        } else {
            return 1.0D - ninv <= d && d < 1.0D?1.0D - 2.0D * Math.pow(1.0D - d, (double)n):(1.0D <= d?1.0D:(exact?this.exactK(d, n):(n <= 140?this.roundedK(d, n):this.pelzGood(d, n))));
        }
    }

    private double exactK(double d, int n) throws MathArithmeticException {
        int k = (int)Math.ceil((double)n * d);
        FieldMatrix H = this.createExactH(d, n);
        FieldMatrix Hpower = H.power(n);
        BigFraction pFrac = (BigFraction)Hpower.getEntry(k - 1, k - 1);

        for(int i = 1; i <= n; ++i) {
            pFrac = pFrac.multiply(i).divide(n);
        }

        return pFrac.bigDecimalValue(20, 4).doubleValue();
    }

    private double roundedK(double d, int n) {
        int k = (int)Math.ceil((double)n * d);
        RealMatrix H = this.createRoundedH(d, n);
        RealMatrix Hpower = H.power(n);
        double pFrac = Hpower.getEntry(k - 1, k - 1);

        for(int i = 1; i <= n; ++i) {
            pFrac *= (double)i / (double)n;
        }

        return pFrac;
    }

    public double pelzGood(double d, int n) {
        double sqrtN = FastMath.sqrt((double)n);
        double z = d * sqrtN;
        double z2 = d * d * (double)n;
        double z4 = z2 * z2;
        double z6 = z4 * z2;
        double z8 = z4 * z4;
        double ret = 0.0D;
        double sum = 0.0D;
        double increment = 0.0D;
        double kTerm = 0.0D;
        double z2Term = 9.869604401089358D / (8.0D * z2);

        int k;
        for(k = 1; k < 100000; ++k) {
            kTerm = (double)(2 * k - 1);
            increment = FastMath.exp(-z2Term * kTerm * kTerm);
            sum += increment;
            if(increment <= 1.0E-10D * sum) {
                break;
            }
        }

        if(k == 100000) {
            throw new TooManyIterationsException(Integer.valueOf(100000));
        } else {
            ret = sum * FastMath.sqrt(6.283185307179586D) / z;
            double twoZ2 = 2.0D * z2;
            sum = 0.0D;
            kTerm = 0.0D;
            double kTerm2 = 0.0D;

            for(k = 0; k < 100000; ++k) {
                kTerm = (double)k + 0.5D;
                kTerm2 = kTerm * kTerm;
                increment = (9.869604401089358D * kTerm2 - z2) * FastMath.exp(-9.869604401089358D * kTerm2 / twoZ2);
                sum += increment;
                if(FastMath.abs(increment) < 1.0E-10D * FastMath.abs(sum)) {
                    break;
                }
            }

            if(k == 100000) {
                throw new TooManyIterationsException(Integer.valueOf(100000));
            } else {
                double sqrtHalfPi = FastMath.sqrt(1.5707963267948966D);
                ret += sum * sqrtHalfPi / (3.0D * z4 * sqrtN);
                double z4Term = 2.0D * z4;
                double z6Term = 6.0D * z6;
                z2Term = 5.0D * z2;
                double pi4 = 97.40909103400243D;
                sum = 0.0D;
                kTerm = 0.0D;
                kTerm2 = 0.0D;

                for(k = 0; k < 100000; ++k) {
                    kTerm = (double)k + 0.5D;
                    kTerm2 = kTerm * kTerm;
                    increment = (z6Term + z4Term + 9.869604401089358D * (z4Term - z2Term) * kTerm2 + 97.40909103400243D * (1.0D - twoZ2) * kTerm2 * kTerm2) * FastMath.exp(-9.869604401089358D * kTerm2 / twoZ2);
                    sum += increment;
                    if(FastMath.abs(increment) < 1.0E-10D * FastMath.abs(sum)) {
                        break;
                    }
                }

                if(k == 100000) {
                    throw new TooManyIterationsException(Integer.valueOf(100000));
                } else {
                    double sum2 = 0.0D;
                    kTerm2 = 0.0D;

                    for(k = 1; k < 100000; ++k) {
                        kTerm2 = (double)(k * k);
                        increment = 9.869604401089358D * kTerm2 * FastMath.exp(-9.869604401089358D * kTerm2 / twoZ2);
                        sum2 += increment;
                        if(FastMath.abs(increment) < 1.0E-10D * FastMath.abs(sum2)) {
                            break;
                        }
                    }

                    if(k == 100000) {
                        throw new TooManyIterationsException(Integer.valueOf(100000));
                    } else {
                        ret += sqrtHalfPi / (double)n * (sum / (36.0D * z2 * z2 * z2 * z) - sum2 / (18.0D * z2 * z));
                        double pi6 = 961.3891935753043D;
                        sum = 0.0D;
                        double kTerm4 = 0.0D;
                        double kTerm6 = 0.0D;

                        for(k = 0; k < 100000; ++k) {
                            kTerm = (double)k + 0.5D;
                            kTerm2 = kTerm * kTerm;
                            kTerm4 = kTerm2 * kTerm2;
                            kTerm6 = kTerm4 * kTerm2;
                            increment = (961.3891935753043D * kTerm6 * (5.0D - 30.0D * z2) + 97.40909103400243D * kTerm4 * (-60.0D * z2 + 212.0D * z4) + 9.869604401089358D * kTerm2 * (135.0D * z4 - 96.0D * z6) - 30.0D * z6 - 90.0D * z8) * FastMath.exp(-9.869604401089358D * kTerm2 / twoZ2);
                            sum += increment;
                            if(FastMath.abs(increment) < 1.0E-10D * FastMath.abs(sum)) {
                                break;
                            }
                        }

                        if(k == 100000) {
                            throw new TooManyIterationsException(Integer.valueOf(100000));
                        } else {
                            sum2 = 0.0D;

                            for(k = 1; k < 100000; ++k) {
                                kTerm2 = (double)(k * k);
                                kTerm4 = kTerm2 * kTerm2;
                                increment = (-97.40909103400243D * kTerm4 + 29.608813203268074D * kTerm2 * z2) * FastMath.exp(-9.869604401089358D * kTerm2 / twoZ2);
                                sum2 += increment;
                                if(FastMath.abs(increment) < 1.0E-10D * FastMath.abs(sum2)) {
                                    break;
                                }
                            }

                            if(k == 100000) {
                                throw new TooManyIterationsException(Integer.valueOf(100000));
                            } else {
                                return ret + sqrtHalfPi / (sqrtN * (double)n) * (sum / (3240.0D * z6 * z4) + sum2 / (108.0D * z6));
                            }
                        }
                    }
                }
            }
        }
    }

    private FieldMatrix<BigFraction> createExactH(double d, int n) throws NumberIsTooLargeException, FractionConversionException {
        int k = (int)Math.ceil((double)n * d);
        int m = 2 * k - 1;
        double hDouble = (double)k - (double)n * d;
        if(hDouble >= 1.0D) {
            throw new NumberIsTooLargeException(Double.valueOf(hDouble), Double.valueOf(1.0D), false);
        } else {
            BigFraction h = null;

            try {
                h = new BigFraction(hDouble, 1.0E-20D, 10000);
            } catch (FractionConversionException var15) {
                try {
                    h = new BigFraction(hDouble, 1.0E-10D, 10000);
                } catch (FractionConversionException var14) {
                    h = new BigFraction(hDouble, 1.0E-5D, 10000);
                }
            }

            BigFraction[][] Hdata = new BigFraction[m][m];

            int i;
            for(int hPowers = 0; hPowers < m; ++hPowers) {
                for(i = 0; i < m; ++i) {
                    if(hPowers - i + 1 < 0) {
                        Hdata[hPowers][i] = BigFraction.ZERO;
                    } else {
                        Hdata[hPowers][i] = BigFraction.ONE;
                    }
                }
            }

            BigFraction[] var16 = new BigFraction[m];
            var16[0] = h;

            for(i = 1; i < m; ++i) {
                var16[i] = h.multiply(var16[i - 1]);
            }

            for(i = 0; i < m; ++i) {
                Hdata[i][0] = Hdata[i][0].subtract(var16[i]);
                Hdata[m - 1][i] = Hdata[m - 1][i].subtract(var16[m - i - 1]);
            }

            if(h.compareTo(BigFraction.ONE_HALF) == 1) {
                Hdata[m - 1][0] = Hdata[m - 1][0].add(h.multiply(2).subtract(1).pow(m));
            }

            for(i = 0; i < m; ++i) {
                for(int j = 0; j < i + 1; ++j) {
                    if(i - j + 1 > 0) {
                        for(int g = 2; g <= i - j + 1; ++g) {
                            Hdata[i][j] = Hdata[i][j].divide(g);
                        }
                    }
                }
            }

            return new Array2DRowFieldMatrix(BigFractionField.getInstance(), Hdata);
        }
    }

    private RealMatrix createRoundedH(double d, int n) throws NumberIsTooLargeException {
        int k = (int)Math.ceil((double)n * d);
        int m = 2 * k - 1;
        double h = (double)k - (double)n * d;
        if(h >= 1.0D) {
            throw new NumberIsTooLargeException(Double.valueOf(h), Double.valueOf(1.0D), false);
        } else {
            double[][] Hdata = new double[m][m];

            int i;
            for(int hPowers = 0; hPowers < m; ++hPowers) {
                for(i = 0; i < m; ++i) {
                    if(hPowers - i + 1 < 0) {
                        Hdata[hPowers][i] = 0.0D;
                    } else {
                        Hdata[hPowers][i] = 1.0D;
                    }
                }
            }

            double[] var13 = new double[m];
            var13[0] = h;

            for(i = 1; i < m; ++i) {
                var13[i] = h * var13[i - 1];
            }

            for(i = 0; i < m; ++i) {
                Hdata[i][0] -= var13[i];
                Hdata[m - 1][i] -= var13[m - i - 1];
            }

            if(Double.compare(h, 0.5D) > 0) {
                Hdata[m - 1][0] += FastMath.pow(2.0D * h - 1.0D, m);
            }

            for(i = 0; i < m; ++i) {
                for(int j = 0; j < i + 1; ++j) {
                    if(i - j + 1 > 0) {
                        for(int g = 2; g <= i - j + 1; ++g) {
                            Hdata[i][j] /= (double)g;
                        }
                    }
                }
            }

            return MatrixUtils.createRealMatrix(Hdata);
        }
    }

    private void checkArray(double[] array) {
        if(array == null) {
            throw new NullArgumentException(LocalizedFormats.NULL_NOT_ALLOWED, new Object[0]);
        } else if(array.length < 2) {
            throw new InsufficientDataException(LocalizedFormats.INSUFFICIENT_OBSERVED_POINTS_IN_SAMPLE, new Object[]{Integer.valueOf(array.length), Integer.valueOf(2)});
        }
    }

    public double ksSum(double t, double tolerance, int maxIterations) {
        if(t == 0.0D) {
            return 0.0D;
        } else {
            double x = -2.0D * t * t;
            int sign = -1;
            long i = 1L;
            double partialSum = 0.5D;

            for(double delta = 1.0D; delta > tolerance && i < (long)maxIterations; ++i) {
                delta = FastMath.exp(x * (double)i * (double)i);
                partialSum += (double)sign * delta;
                sign *= -1;
            }

            if(i == (long)maxIterations) {
                throw new TooManyIterationsException(Integer.valueOf(maxIterations));
            } else {
                return partialSum * 2.0D;
            }
        }
    }

    private static long calculateIntegralD(double d, int n, int m, boolean strict) {
        double tol = 1.0E-12D;
        long nm = (long)n * (long)m;
        long upperBound = (long) FastMath.ceil((d - 1.0E-12D) * (double)nm);
        long lowerBound = (long) FastMath.floor((d + 1.0E-12D) * (double)nm);
        return strict && lowerBound == upperBound?upperBound + 1L:upperBound;
    }

    public double exactP(double d, int n, int m, boolean strict) {
        return 1.0D - n(m, n, m, n, calculateIntegralD(d, m, n, strict), strict) / CombinatoricsUtils.binomialCoefficientDouble(n + m, m);
    }

    public double approximateP(double d, int n, int m) {
        double dm = (double)m;
        double dn = (double)n;
        return 1.0D - this.ksSum(d * FastMath.sqrt(dm * dn / (dm + dn)), 1.0E-20D, 100000);
    }

    static void fillBooleanArrayRandomlyWithFixedNumberTrueValues(boolean[] b, int numberOfTrueValues, RandomGenerator rng) {
        Arrays.fill(b, true);

        for(int k = numberOfTrueValues; k < b.length; ++k) {
            int r = rng.nextInt(k + 1);
            b[b[r]?r:k] = false;
        }

    }

    public double monteCarloP(double d, int n, int m, boolean strict, int iterations) {
        return this.integralMonteCarloP(calculateIntegralD(d, n, m, strict), n, m, iterations);
    }

    private double integralMonteCarloP(long d, int n, int m, int iterations) {
        int nn = FastMath.max(n, m);
        int mm = FastMath.min(n, m);
        int sum = nn + mm;
        int tail = 0;
        boolean[] b = new boolean[sum];

        for(int i = 0; i < iterations; ++i) {
            fillBooleanArrayRandomlyWithFixedNumberTrueValues(b, nn, this.rng);
            long curD = 0L;

            for(int j = 0; j < b.length; ++j) {
                if(b[j]) {
                    curD += (long)mm;
                    if(curD >= d) {
                        ++tail;
                        break;
                    }
                } else {
                    curD -= (long)nn;
                    if(curD <= -d) {
                        ++tail;
                        break;
                    }
                }
            }
        }

        return (double)tail / (double)iterations;
    }

    private static void fixTies(double[] x, double[] y) {
        double[] values = MathArrays.unique(MathArrays.concatenate(new double[][]{x, y}));
        if(values.length != x.length + y.length) {
            double minDelta = 1.0D;
            double prev = values[0];
            double delta = 1.0D;

            for(int dist = 1; dist < values.length; ++dist) {
                delta = prev - values[dist];
                if(delta < minDelta) {
                    minDelta = delta;
                }

                prev = values[dist];
            }

            minDelta /= 2.0D;
            UniformRealDistribution var12 = new UniformRealDistribution(new JDKRandomGenerator(100), -minDelta, minDelta);
            int ct = 0;
            boolean ties = true;

            do {
                jitter(x, var12);
                jitter(y, var12);
                ties = hasTies(x, y);
                ++ct;
            } while(ties && ct < 1000);

            if(ties) {
                throw new MathInternalError();
            }
        }
    }

    private static boolean hasTies(double[] x, double[] y) {
        HashSet values = new HashSet();

        int i;
        for(i = 0; i < x.length; ++i) {
            if(!values.add(Double.valueOf(x[i]))) {
                return true;
            }
        }

        for(i = 0; i < y.length; ++i) {
            if(!values.add(Double.valueOf(y[i]))) {
                return true;
            }
        }

        return false;
    }

    private static void jitter(double[] data, RealDistribution dist) {
        for(int i = 0; i < data.length; ++i) {
            data[i] += dist.sample();
        }

    }

    private static int c(int i, int j, int m, int n, long cmn, boolean strict) {
        return strict?(FastMath.abs((long)i * (long)n - (long)j * (long)m) <= cmn?1:0):(FastMath.abs((long)i * (long)n - (long)j * (long)m) < cmn?1:0);
    }

    private static double n(int i, int j, int m, int n, long cnm, boolean strict) {
        double[] lag = new double[n];
        double last = 0.0D;

        int k;
        for(k = 0; k < n; ++k) {
            lag[k] = (double)c(0, k + 1, m, n, cnm, strict);
        }

        for(k = 1; k <= i; ++k) {
            last = (double)c(k, 0, m, n, cnm, strict);

            for(int l = 1; l <= j; ++l) {
                lag[l - 1] = (double)c(k, l, m, n, cnm, strict) * (last + lag[l - 1]);
                last = lag[l - 1];
            }
        }

        return last;
    }
}

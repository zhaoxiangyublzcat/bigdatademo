public class MIN extends UDF {
    public Double evaluate(Double a, Double b) {
        if (a == null) {
            a = 0.0;
        }

        if (b == null) {
            b = 0.0;
        }

        if (a >= b) {
            return b;
        } else {
            return a;
        }
    }
}

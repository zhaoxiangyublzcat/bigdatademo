public class UDF {
    private String tradedate;
    private String tradetime;
    private String stockid;
    private double buyprice;
    private int buysize;
    private double sellprice;
    private int sellsize;

    public UDF() {
    }

    public String getTradedate() {
        return tradedate;
    }

    public void setTradedate(String tradedate) {
        this.tradedate = tradedate;
    }

    public String getTradetime() {
        return tradetime;
    }

    public void setTradetime(String tradetime) {
        this.tradetime = tradetime;
    }

    public String getStockid() {
        return stockid;
    }

    public void setStockid(String stockid) {
        this.stockid = stockid;
    }

    public double getBuyprice() {
        return buyprice;
    }

    public void setBuyprice(double buyprice) {
        this.buyprice = buyprice;
    }

    public int getBuysize() {
        return buysize;
    }

    public void setBuysize(int buysize) {
        this.buysize = buysize;
    }

    public double getSellprice() {
        return sellprice;
    }

    public void setSellprice(double sellprice) {
        this.sellprice = sellprice;
    }

    public int getSellsize() {
        return sellsize;
    }

    public void setSellsize(int sellsize) {
        this.sellsize = sellsize;
    }
}

package pojo;

import com.google.gson.Gson;

import java.math.BigDecimal;

public class Transaction {
    private String transactionId;
    private Long timestamp;
    private String sendingClientName;
    private String sendingClientAccountNumber;
    private String receivingClientName;
    private String receivingClientAccountNumber;
    private BigDecimal amount;

    public Transaction() {
    }

    public Transaction(String transactionId, String timestamp, String sendingClientAccountNumber, String receivingClientAccountNumber, BigDecimal amount) {
        this.transactionId = transactionId;
        this.timestamp = Long.valueOf(timestamp);
        this.sendingClientAccountNumber = sendingClientAccountNumber;
        this.receivingClientAccountNumber = receivingClientAccountNumber;
        this.amount = amount;
    }

//    public Transaction(String sendingClientName, String sendingClientAccountNumber, String receivingClientName, String receivingClientAccountNumber, BigDecimal amount) {
//        this.sendingClientName = sendingClientName;
//        this.sendingClientAccountNumber = sendingClientAccountNumber;
//        this.receivingClientName = receivingClientName;
//        this.receivingClientAccountNumber = receivingClientAccountNumber;
//        this.amount = amount;
//    }


    public String getTransactionId(){
        return transactionId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getSendingClientName() {
        return sendingClientName;
    }

    public String getSendingClientAccountNumber() {
        return sendingClientAccountNumber;
    }

    public String getReceivingClientName() {
        return receivingClientName;
    }

    public String getReceivingClientAccountNumber() {
        return receivingClientAccountNumber;
    }

    public double getAmount() {
        return amount.doubleValue();
    }

    @Override
    public String toString(){
        return new Gson().toJson(this, Transaction.class);
    }
}

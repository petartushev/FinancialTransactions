package pojo;

import java.math.BigDecimal;

public class Account {
    private String clientName;
    private Long clientEMBG;
    private Long accountId;
    private BigDecimal balance;

    public Account() {
    }

    public Account(String clientName, Long clientEMBG, Long accountId, BigDecimal balance) {
        this.clientName = clientName;
        this.clientEMBG = clientEMBG;
        this.accountId = accountId;
        this.balance = balance;
    }

    public String getClientName() {
        return clientName;
    }

    public Long getClientEMBG() {
        return clientEMBG;
    }

    public Long getAccountId() {
        return accountId;
    }

    public BigDecimal getBalance() {
        return balance;
    }
}

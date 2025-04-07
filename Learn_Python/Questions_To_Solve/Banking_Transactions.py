"""
Create a simple Python script to manage banking transactions for a small bank. The script should be able to record deposits and withdrawals, and it should validate that each transaction updates the account balance correctly.
Input: transactions = [ ("deposit", 100), ("withdraw", 50), ("withdraw", 70), ("deposit", 200),]
has context menu
"""

class BankTransactions:
    def __init__(self, initial_balance=500):
        self.balance = initial_balance
        self.min_balance = 500

    def deposit(self, amount):
        if amount>0:
            self.balance+=amount
            print(f"Balance after amount: {amount} was deposited = {self.balance}")
        else:
            print("Amount must be positive")

    def withdraw(self, amount):
        if (self.balance-self.min_balance) >= amount:
            self.balance-=amount
            print(f"Your current balance = {self.balance} after deducting amount={amount}")
        else:
            print("Balance is insufficient for withdrawal. Check your balance once")

    def getBalance(self):
        return self.balance

    def processTransactions(self, transactions):
        for transaction in transactions:
            action, amount = transaction
            if action == "deposit":
                self.deposit(amount)
            elif action == "withdrawal":
                self.withdraw(amount)
            else:
                print(f"Invalid transactions type: {action}")


if __name__ == '__main__':
    bankObj = BankTransactions()

    transactions = [
        ("deposit", 100),
        ("deposit", 100),
        ("withdrawal", 200),
        ("withdrawal", 200)
    ]

    bankObj.processTransactions(transactions)

    bankObj.getBalance()








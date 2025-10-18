#!/usr/bin/env python3
"""
Simple stock investment ledger using pgledger
"""
import psycopg
from datetime import datetime
from decimal import Decimal

# Database connection
DB_CONFIG = {
    'dbname': 'pgledger',
    'user': 'pgledger',
    'password': 'pgledger',
    'host': 'localhost',
    'port': 5432
}

class StockLedger:
    def __init__(self):
        self.conn = psycopg.connect(**DB_CONFIG)
        self.conn.autocommit = False
        self.accounts = {}
        
    def setup(self):
        """Initialize accounts"""
        cur = self.conn.cursor()
        
        # Create accounts
        accounts_to_create = [
            ('user.KRW', 'KRW'),
            ('user.USD', 'USD'),
            ('user.GOOGL', 'GOOGL'),
            ('liquidity.KRW', 'KRW'),
            ('liquidity.USD', 'USD'),
            ('liquidity.GOOGL', 'GOOGL'),
        ]
        
        for name, currency in accounts_to_create:
            cur.execute(
                "SELECT id FROM pgledger_create_account(%s, %s, TRUE, TRUE)",
                (name, currency)
            )
            account_id = cur.fetchone()[0]
            self.accounts[name] = account_id
            print(f"Created: {name} ({account_id})")
        
        self.conn.commit()
        
    def forex(self, krw_amount, usd_amount):
        """환전: KRW -> USD"""
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT * FROM pgledger_create_transfers(
                ROW(%s, %s, %s)::transfer_request,
                ROW(%s, %s, %s)::transfer_request
            )
        """, (
            self.accounts['user.KRW'], self.accounts['liquidity.KRW'], krw_amount,
            self.accounts['liquidity.USD'], self.accounts['user.USD'], usd_amount
        ))
        
        self.conn.commit()
        rate = float(krw_amount) / float(usd_amount)
        print(f"환전: {krw_amount} KRW -> {usd_amount} USD (환율: {rate:.2f})")
        
    def buy_stock(self, shares, price_per_share):
        """주식 매수"""
        cur = self.conn.cursor()
        total_usd = Decimal(shares) * Decimal(price_per_share)
        
        cur.execute("""
            SELECT * FROM pgledger_create_transfers(
                ROW(%s, %s, %s)::transfer_request,
                ROW(%s, %s, %s)::transfer_request
            )
        """, (
            self.accounts['user.USD'], self.accounts['liquidity.USD'], str(total_usd),
            self.accounts['liquidity.GOOGL'], self.accounts['user.GOOGL'], str(shares)
        ))
        
        self.conn.commit()
        print(f"매수: {shares}주 @ ${price_per_share} = ${total_usd}")
        
    def sell_stock(self, shares, price_per_share):
        """주식 매도"""
        cur = self.conn.cursor()
        total_usd = Decimal(shares) * Decimal(price_per_share)
        
        cur.execute("""
            SELECT * FROM pgledger_create_transfers(
                ROW(%s, %s, %s)::transfer_request,
                ROW(%s, %s, %s)::transfer_request
            )
        """, (
            self.accounts['user.GOOGL'], self.accounts['liquidity.GOOGL'], str(shares),
            self.accounts['liquidity.USD'], self.accounts['user.USD'], str(total_usd)
        ))
        
        self.conn.commit()
        print(f"매도: {shares}주 @ ${price_per_share} = ${total_usd}")
        
    def show_balances(self):
        """잔고 조회"""
        cur = self.conn.cursor()
        
        print("\n=== 잔고 ===")
        for name in ['user.KRW', 'user.USD', 'user.GOOGL']:
            cur.execute(
                "SELECT balance, version FROM pgledger_accounts_view WHERE id = %s",
                (self.accounts[name],)
            )
            balance, version = cur.fetchone()
            print(f"{name:15} {balance:>12} (v{version})")
    
    def show_history(self, account_name):
        """거래 내역 조회"""
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT 
                created_at,
                amount,
                account_previous_balance,
                account_current_balance
            FROM pgledger_entries_view
            WHERE account_id = %s
            ORDER BY created_at
        """, (self.accounts[account_name],))
        
        print(f"\n=== {account_name} 거래내역 ===")
        for row in cur.fetchall():
            created, amount, prev, curr = row
            print(f"{created} | {amount:>10} | {prev:>10} -> {curr:>10}")
    
    def close(self):
        self.conn.close()


def main():
    ledger = StockLedger()
    
    try:
        # 1. 계정 생성
        print("1. 계정 생성")
        ledger.setup()
        
        # 2. 초기 자금 입금 (KRW 1천만원 가정)
        print("\n2. 초기 자금 입금")
        cur = ledger.conn.cursor()
        cur.execute("""
            SELECT * FROM pgledger_create_transfer(%s, %s, %s)
        """, (
            ledger.accounts['liquidity.KRW'],
            ledger.accounts['user.KRW'],
            '10000000'
        ))
        ledger.conn.commit()
        print("입금: 10,000,000 KRW")
        
        # 3. 환전
        print("\n3. 환전")
        ledger.forex('1300000', '1000')  # 1,300원/달러
        
        # 4. 주식 매수
        print("\n4. 주식 매수")
        ledger.buy_stock('5', '150.50')  # 5주 @ $150.50
        
        # 5. 잔고 확인
        ledger.show_balances()
        
        # 6. 주식 매도
        print("\n6. 주식 매도")
        ledger.sell_stock('2', '160.00')  # 2주 @ $160.00
        
        # 7. 최종 잔고
        ledger.show_balances()
        
        # 8. USD 거래내역
        ledger.show_history('user.USD')
        
    finally:
        ledger.close()


if __name__ == '__main__':
    main()
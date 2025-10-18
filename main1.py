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

#!/usr/bin/env python3
"""
Simple stock investment ledger using pgledger
"""
import psycopg
from datetime import datetime
from decimal import Decimal
import sys

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
        self.load_accounts() # 인스턴스 생성 시 기존 계정 로드
        
    def load_accounts(self):
        """데이터베이스에서 모든 계정 정보를 로드하여 self.accounts에 저장"""
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM pgledger_accounts_view ORDER BY name")
        
        self.accounts = {}
        for account_id, name in cur.fetchall():
            self.accounts[name] = account_id
        
        if self.accounts:
            print(f"✅ 기존 계정 {len(self.accounts)}개 로드 완료.")
            
    def create_account(self, name, currency):
        """새 계정 생성 (중복 방지 로직 포함)"""
        cur = self.conn.cursor()
        
        # 1. 계정 이름 중복 확인
        if name in self.accounts:
            print(f"❌ 오류: 계정 '{name}'은(는) 이미 존재합니다.")
            return False
            
        try:
            # 2. PG Ledger 함수를 사용하여 계정 생성
            cur.execute(
                "SELECT id FROM pgledger_create_account(%s, %s, TRUE, TRUE)",
                (name, currency.upper())
            )
            account_id = cur.fetchone()[0]
            
            # 3. 인스턴스 변수에 ID 저장 및 커밋
            self.accounts[name] = account_id
            self.conn.commit()
            print(f"✅ 계정 생성 성공: {name} ({currency.upper()}) [ID: {account_id}]")
            return True
            
        except psycopg.Error as e:
            self.conn.rollback()
            print(f"❌ 데이터베이스 오류 발생: {e}")
            return False

    def show_all_accounts(self):
        """현재 모든 계정의 이름, ID, 잔고를 조회"""
        if not self.accounts:
            print("\n🚨 등록된 계정이 없습니다.")
            return
            
        cur = self.conn.cursor()
        
        print("\n=== 등록된 계정 및 잔고 ===")
        print(f"{'계정 이름':<20} {'ID':<5} {'잔고':>15} {'버전':>5}")
        print("-" * 45)
        
        # 정렬된 계정 이름 목록을 순회
        sorted_names = sorted(self.accounts.keys())
        
        for name in sorted_names:
            account_id = self.accounts[name]
            cur.execute(
                "SELECT balance, version FROM pgledger_accounts_view WHERE id = %s",
                (account_id,)
            )
            balance, version = cur.fetchone()
            print(f"{name:<20} {account_id:<5} {balance:>15} (v{version})")

    def close(self):
        self.conn.close()

def display_menu():
    """메인 메뉴 출력"""
    print("\n" + "="*30)
    print("💰 PG Ledger 기반 투자 장부 관리 시스템")
    print("="*30)
    print("1. 계정 잔고 및 목록 조회")
    print("2. 새 계정 생성")
    print("3. 종료")
    print("-" * 30)

def main():
    try:
        ledger = StockLedger()
    except psycopg.OperationalError as e:
        print(f"\nFATAL: 데이터베이스 연결 실패. DB 설정({DB_CONFIG['dbname']}@{DB_CONFIG['host']})을 확인하세요.")
        print(f"에러: {e}")
        sys.exit(1)

    while True:
        display_menu()
        choice = input("메뉴 선택 (1-3): ")
        
        if choice == '1':
            ledger.show_all_accounts()
            
        elif choice == '2':
            print("\n--- 새 계정 생성 ---")
            name = input("생성할 계정 이름 (예: user.KRW): ")
            currency = input("사용할 통화/자산 코드 (예: KRW, USD, GOOGL): ")
            
            if name and currency:
                ledger.create_account(name.strip(), currency.strip())
            else:
                print("❌ 계정 이름과 통화 코드를 모두 입력해야 합니다.")
                
        elif choice == '3':
            print("\n시스템을 종료합니다. 감사합니다.")
            break
            
        else:
            print("❗ 잘못된 입력입니다. 1, 2, 3 중 하나를 선택하세요.")

    ledger.close()

if __name__ == '__main__':
    main()
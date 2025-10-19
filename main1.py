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

# 새로 정의된 계정 등록 설정
ASSET_TYPES = {
    '1': ('KRW', "원화"),
    '2': ('USD', "미국 달러"),
    '3': ('JPY', "일본 엔화"),
}

ACCOUNT_GROUPS = {
    '1': 'bank',
    # 추후 'investment', 'liability' 등 추가 가능
}

BANK_NAMES = {
    '1': 'woori',
    '2': 'toss',
    '3': 'kb',
    '4': 'hana',
    '5': 'ibk',
    '6': 'test'
}


class StockLedger:
    def __init__(self):
        self.conn = psycopg.connect(**DB_CONFIG)
        self.conn.autocommit = False
        self.accounts = {}
        self.load_accounts()
        
    def load_accounts(self):
        """데이터베이스에서 모든 계정 정보를 로드하여 self.accounts에 저장"""
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM pgledger_accounts_view ORDER BY name")
        
        self.accounts = {}
        for account_id, name in cur.fetchall():
            self.accounts[name] = account_id
        
        if self.accounts:
            print(f"✅ 기존 계정 {len(self.accounts)}개 로드 완료.")
            
    def _create_single_account(self, name, currency):
        """
        [내부 사용] 단일 계정 생성 로직. 
        실제 DB에 계정을 생성하고 성공 여부를 반환합니다.
        """
        cur = self.conn.cursor()
        
        # 1. 계정 이름 중복 확인
        if name in self.accounts:
            return False, self.accounts[name]
            
        try:
            # 2. PG Ledger 함수를 사용하여 계정 생성
            cur.execute(
                "SELECT id FROM pgledger_create_account(%s, %s, TRUE, TRUE)",
                (name, currency.upper())
            )
            account_id = cur.fetchone()[0]
            
            # 3. 인스턴스 변수에 ID 저장
            self.accounts[name] = account_id
            return True, account_id
            
        except psycopg.Error as e:
            print(f"❌ 데이터베이스 오류 발생 ({name}): {e}")
            return False, None

    def create_asset_pair_by_menu(self, group, currency, detail, last_four_digits):
        """
        메뉴 입력값을 받아 'bank.KRW.woori.8472' 형식의 계정 pair를 생성합니다.
        """
        # 자산 계정 이름 (예: bank.KRW.woori.8472)
        asset_name = f"{group}.{currency}.{detail}.{last_four_digits}"
        # 상대 유동성 계정 이름 (예: liquidity.KRW.woori.8472)
        liquidity_name = f"liquidity.{currency}.{detail}.{last_four_digits}"

        print(f"\n--- 계좌 pair 생성 시도 (자산: {currency}, 그룹: {group}.{detail}) ---")

        # 1. 자산 계정 생성 시도
        asset_created, asset_id = self._create_single_account(asset_name, currency)
        if asset_created:
            print(f"  ✅ 자산 계정 생성 성공: {asset_name} [ID: {asset_id}]")
        elif asset_id:
            print(f"  ⚠️ 자산 계정은 이미 존재합니다: {asset_name} [ID: {asset_id}]")
        
        # 2. 유동성 계정 생성 시도
        liquidity_created, liquidity_id = self._create_single_account(liquidity_name, currency)
        if liquidity_created:
            print(f"  ✅ 상대 계정 생성 성공: {liquidity_name} [ID: {liquidity_id}]")
        elif liquidity_id:
            print(f"  ⚠️ 상대 계정은 이미 존재합니다: {liquidity_name} [ID: {liquidity_id}]")
        
        # 3. 최종 커밋 
        if asset_created or liquidity_created:
            self.conn.commit()
            print("  🎉 계좌 pair 설정 완료 (DB 커밋).")
        else:
            print("  ✔️ 변경 사항 없음. (두 계정 모두 이미 존재함).")

    def record_transaction(self, from_account_id, to_account_id, amount):
        """
        지정된 계좌 간의 거래를 기록합니다.
        pgledger_create_transfer 함수를 호출하여 DB에 반영합니다.
        """
        cur = self.conn.cursor()
        try:
            print(f"\n--- 거래 실행: {from_account_id} -> {to_account_id} (금액: {amount}) ---")
            
            cur.execute(
                "SELECT id FROM pgledger_create_transfer(%s, %s, %s)",
                (from_account_id, to_account_id, amount)
            )
            
            transfer_id = cur.fetchone()[0]
            self.conn.commit()
            
            print(f"  ✅ 거래 성공! [Transfer ID: {transfer_id}]")
            return True

        except psycopg.Error as e:
            print(f"  ❌ 거래 실패: {e}")
            self.conn.rollback()
            return False


    def show_all_accounts(self):
        """현재 모든 계정의 이름, ID, 잔고를 조회"""
        if not self.accounts:
            print("\n🚨 등록된 계정이 없습니다.")
            return
            
        cur = self.conn.cursor()
        
        print("\n=== 등록된 계정 및 잔고 ===")
        print(f"{'계정 이름':<30} {'ID':<5} {'잔고':>15} {'버전':>5}")
        print("-" * 55)
        
        sorted_names = sorted(self.accounts.keys())
        
        for name in sorted_names:
            account_id = self.accounts[name]
            cur.execute(
                "SELECT balance, version FROM pgledger_accounts_view WHERE id = %s",
                (account_id,)
            )
            balance, version = cur.fetchone()
            print(f"{name:<30} {account_id:<5} {balance:>15} (v{version})")

    def close(self):
        self.conn.close()


def process_transaction(ledger):
    """3. 거래 기록 메뉴의 워크플로우를 처리하는 함수"""
    
    if not ledger.accounts:
        print("\n🚨 거래를 기록하기 전에 먼저 계좌를 등록해야 합니다.")
        return

    # 이름순으로 정렬된 전체 계좌 목록
    all_accounts = sorted(ledger.accounts.items(), key=lambda item: item[0])
    
    # 기본적으로 보여줄 계좌 목록 (liquidity 제외)
    visible_accounts = [(name, acc_id) for name, acc_id in all_accounts if not name.startswith('liquidity.')]

    while True:
        print("\n--- 3. 거래 기록 (계좌 선택) ---")
        cur = ledger.conn.cursor()
        print(f"{'번호':<5} {'계정 이름':<30} {'현재 잔고':>15}")
        print("-" * 55)
        
        for i, (name, account_id) in enumerate(visible_accounts):
            cur.execute("SELECT balance FROM pgledger_accounts_view WHERE id = %s", (account_id,))
            balance = cur.fetchone()[0]
            print(f"{i + 1:<5} {name:<30} {balance:>15,.0f}")
        
        print("-" * 55)
        if len(visible_accounts) < len(all_accounts):
            print("99. 모든 계좌 보기 (liquidity 포함)")

        try:
            from_choice_str = input("\n어떤 계좌에서 출금하시겠습니까? (번호 입력): ").strip()
            
            if from_choice_str == '99' and len(visible_accounts) < len(all_accounts):
                visible_accounts = all_accounts
                continue # 메뉴를 다시 보여줌

            from_choice = int(from_choice_str) - 1
            if not 0 <= from_choice < len(visible_accounts):
                print("❗ 잘못된 번호입니다.")
                continue
            
            from_account_id = visible_accounts[from_choice][1]
            from_account_name = visible_accounts[from_choice][0]
            break # 출금 계좌 선택 완료

        except ValueError:
            print("❗ 숫자로 입력해주세요.")
            continue

    # 3. 입금 계좌 선택
    try:
        to_choice = int(input(f"어떤 계좌로 입금하시겠습니까? (번호 입력): ")) - 1
        if not 0 <= to_choice < len(visible_accounts):
            print("❗ 잘못된 번호입니다.")
            return
            
        if visible_accounts[from_choice][1] == visible_accounts[to_choice][1]:
            print("❗ 출금 계좌와 입금 계좌는 같을 수 없습니다.")
            return

        to_account_id = visible_accounts[to_choice][1]
        to_account_name = visible_accounts[to_choice][0]

    except ValueError:
        print("❗ 숫자로 입력해주세요.")
        return

    # 4. 금액 입력
    try:
        amount_str = input(f"얼마를 이체하시겠습니까? ({from_account_name} -> {to_account_name}): ").strip()
        amount = Decimal(amount_str)
        
        if amount <= 0:
            print("❗ 금액은 0보다 커야 합니다.")
            return
            
    except Exception:
        print("❗ 유효한 금액을 입력해주세요.")
        return

    # 5. 거래 실행
    ledger.record_transaction(from_account_id, to_account_id, amount)


def process_account_registration(ledger):
    """2. 계좌 등록 메뉴의 워크플로우를 처리하는 함수"""
    
    # 1. 통화 선택
    print("\n--- 2. 계좌 등록 (통화 선택) ---")
    for key, (code, name) in ASSET_TYPES.items():
        print(f"{key}. {name} ({code})")
    
    currency_choice = input("통화 선택 (1-3): ")
    if currency_choice not in ASSET_TYPES:
        print("❗ 잘못된 통화 선택입니다.")
        return

    currency_code = ASSET_TYPES[currency_choice][0]

    # 2. 그룹 (Type) 선택
    print("\n--- 2. 계좌 등록 (계정 그룹 선택) ---")
    for key, name in ACCOUNT_GROUPS.items():
        print(f"{key}. {name.capitalize()}")
    
    group_choice = input("그룹 선택 (1): ")
    if group_choice not in ACCOUNT_GROUPS:
        print("❗ 잘못된 그룹 선택입니다.")
        return
    
    group_name = ACCOUNT_GROUPS[group_choice]

    # 3. 상세 기관 (Bank) 선택
    print("\n--- 2. 계좌 등록 (상세 기관 선택) ---")
    for key, name in BANK_NAMES.items():
        print(f"{key}. {name.capitalize()}")
    
    detail_choice = input("기관 선택 (1-5): ")
    if detail_choice not in BANK_NAMES:
        print("❗ 잘못된 기관 선택입니다.")
        return
        
    detail_name = BANK_NAMES[detail_choice]

    # 4. 세부 계좌 번호 입력
    last_four_digits = input(f"\n세부 계좌 끝자리를 입력하세요 (예: 8472): ").strip()
    
    if not last_four_digits or not last_four_digits.isdigit() or len(last_four_digits) > 10:
        print("❌ 유효하지 않은 계좌 끝자리입니다. 숫자로 입력해주세요.")
        return
        
    # 5. 계좌 pair 생성 호출
    ledger.create_asset_pair_by_menu(group_name, currency_code, detail_name, last_four_digits)


def display_menu():
    """메인 메뉴 출력"""
    print("\n" + "="*40)
    print("💰 복식부기 장부 관리 시스템")
    print("="*40)
    print("1. 계정 목록 및 잔고 조회")
    print("2. 계좌 등록 (은행, 증권사)")
    print("3. 거래 기록")
    print("4. 종료")
    print("-" * 40)

def main():
    try:
        ledger = StockLedger()
    except psycopg.OperationalError as e:
        print(f"\nFATAL: 데이터베이스 연결 실패. DB 설정({DB_CONFIG['dbname']}@{DB_CONFIG['host']})을 확인하세요.")
        print(f"에러: {e}")
        sys.exit(1)

    while True:
        display_menu()
        choice = input("메뉴 선택 (1-4): ")
        
        if choice == '1':
            ledger.show_all_accounts()
            
        elif choice == '2':
            process_account_registration(ledger)

        elif choice == '3':
            process_transaction(ledger)
                
        elif choice == '4':
            print("\n시스템을 종료합니다. 감사합니다.")
            break
            
        else:
            print("❗ 잘못된 입력입니다. 1, 2, 3, 4 중 하나를 선택하세요.")

    ledger.close()

if __name__ == '__main__':
    main()
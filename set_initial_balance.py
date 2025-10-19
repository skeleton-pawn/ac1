#!/usr/bin/env python3
"""
PG Ledger: 초기 잔고를 직접 수정하는 스크립트
데이터베이스 테이블을 직접 UPDATE하여 잔고와 날짜를 설정합니다.
"""
import psycopg
import sys
from datetime import datetime, timezone, timedelta

# 데이터베이스 연결 설정
DB_CONFIG = {
    'dbname': 'pgledger',
    'user': 'pgledger',
    'password': 'pgledger',
    'host': 'localhost',
    'port': 5432
}

def get_account_info(cur, account_name):
    """계정 이름으로 전체 정보를 조회합니다."""
    cur.execute(
        "SELECT id, name, currency, balance FROM pgledger_accounts_view WHERE name = %s", 
        (account_name,)
    )
    return cur.fetchone()

def get_modifiable_accounts():
    """데이터베이스에서 'bank.'로 시작하며 잔고가 0인 모든 계정 목록을 조회합니다."""
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT name, currency, balance
            FROM pgledger_accounts_view 
            WHERE name LIKE 'bank.%' AND balance = 0
            ORDER BY name
        """)
        return cur.fetchall()
    except psycopg.Error as e:
        print(f"❌ 데이터베이스 오류 발생: 계좌 목록을 불러올 수 없습니다. {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_account_balance_direct(account_name, amount, event_date_str):
    """
    계정의 잔고를 직접 UPDATE하고 거래 기록을 특정 날짜로 생성합니다.
    """
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        # 1. 계정 정보 조회
        account_info = get_account_info(cur, account_name)
        if not account_info:
            print(f"❌ 오류: 계정 '{account_name}'을 찾을 수 없습니다.")
            conn.close()
            return False
            
        account_id, name, currency, current_balance = account_info
        print(f"✅ 계정 정보: ID={account_id}, 이름={name}, 통화={currency}, 현재잔고={current_balance}")
        
        # 2. 상대 계정 (liquidity) 정보 조회
        liquidity_name = "liquidity" + account_name[4:]
        liquidity_info = get_account_info(cur, liquidity_name)
        if not liquidity_info:
            print(f"❌ 오류: 상대 계정 '{liquidity_name}'을 찾을 수 없습니다.")
            conn.close()
            return False
            
        liquidity_id = liquidity_info[0]
        print(f"✅ 상대 계정: ID={liquidity_id}, 이름={liquidity_name}")
        
        # 3. 날짜 파싱 (KST 02:00:00로 설정)
        kst = timezone(timedelta(hours=9))
        event_datetime = datetime.strptime(event_date_str, "%Y-%m-%d")
        event_datetime = event_datetime.replace(hour=2, minute=0, second=0, tzinfo=kst)
        print(f"✅ 이벤트 시각: {event_datetime}")
        
        # 4. pgledger_accounts 테이블 직접 업데이트
        print(f"\n🔧 계정 잔고 업데이트 중...")
        
        # 자산 계정 업데이트 (잔고 증가)
        cur.execute("""
            UPDATE pgledger_accounts 
            SET balance = balance + %s,
                version = version + 1,
                updated_at = %s
            WHERE id = %s
        """, (amount, event_datetime, account_id))
        
        # 상대 계정 업데이트 (잔고 감소)
        cur.execute("""
            UPDATE pgledger_accounts 
            SET balance = balance - %s,
                version = version + 1,
                updated_at = %s
            WHERE id = %s
        """, (amount, event_datetime, liquidity_id))
        
        print(f"   ✅ {account_name}: 잔고 {current_balance} → {current_balance + amount}")
        
        # 5. pgledger_transfers 테이블에 거래 기록 생성
        print(f"\n🔧 거래 기록 생성 중...")
        cur.execute("""
            INSERT INTO pgledger_transfers 
            (from_account_id, to_account_id, amount, created_at, event_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (liquidity_id, account_id, amount, event_datetime, event_datetime))
        
        transfer_id = cur.fetchone()[0]
        print(f"   ✅ Transfer ID: {transfer_id}")
        
        # 6. pgledger_entries 테이블에 엔트리 생성
        print(f"\n🔧 엔트리 기록 생성 중...")
        
        # 출금 엔트리 (liquidity 계정)
        cur.execute("""
            INSERT INTO pgledger_entries 
            (account_id, transfer_id, amount, account_previous_balance, 
             account_current_balance, account_version, created_at)
            VALUES (%s, %s, %s, %s, %s, 
                    (SELECT version FROM pgledger_accounts WHERE id = %s), 
                    %s)
        """, (liquidity_id, transfer_id, -amount, amount, 0, liquidity_id, event_datetime))
        
        # 입금 엔트리 (자산 계정)
        cur.execute("""
            INSERT INTO pgledger_entries 
            (account_id, transfer_id, amount, account_previous_balance, 
             account_current_balance, account_version, created_at)
            VALUES (%s, %s, %s, %s, %s, 
                    (SELECT version FROM pgledger_accounts WHERE id = %s), 
                    %s)
        """, (account_id, transfer_id, amount, 0, amount, account_id, event_datetime))
        
        print(f"   ✅ 엔트리 생성 완료")
        
        # 7. 커밋
        conn.commit()
        print(f"\n🎉 성공: 계정 '{account_name}'의 잔고가 {amount} {currency}로 설정되었습니다.")
        print(f"   - 이벤트 날짜: {event_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return True

    except psycopg.Error as e:
        if conn:
            conn.rollback()
        print(f"\n❌ 데이터베이스 오류: {e}")
        return False
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n❌ 일반 오류: {e}")
        return False
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    print("\n" + "="*60)
    print("      초기 잔고 직접 수정 스크립트")
    print("="*60)
    print("⚠️  주의: 이 스크립트는 데이터베이스를 직접 수정합니다.")
    print("="*60 + "\n")
    
    # 1. 잔고 설정 가능 계정 목록 조회 및 선택
    accounts = get_modifiable_accounts()
    
    if not accounts:
        print("🚨 잔고를 설정할 수 있는 'bank.' 계좌가 없거나, 모든 계좌의 잔고가 0이 아닙니다.")
        sys.exit(1)
        
    print("--- 잔고가 0인 계좌 목록 (수정 가능) ---\n")
    for i, (name, currency, balance) in enumerate(accounts):
        print(f"  {i+1}. {name} ({currency}) - 현재 잔고: {balance}")
        
    print()
    account_name = None
    while True:
        try:
            choice = input("계좌 번호 선택: ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(accounts):
                account_name = accounts[idx][0]
                print(f"✅ {account_name} 선택 완료.\n")
                break
            else:
                print("❗ 잘못된 번호입니다. 목록 내의 번호를 선택해 주세요.")
        except ValueError:
            print("❗ 숫자를 입력해 주세요.")
        except KeyboardInterrupt:
            print("\n\n❌ 사용자가 취소했습니다.")
            sys.exit(0)

    # 2. 금액 입력
    try:
        amount = int(input("초기 잔고 금액 입력 (정수): ").strip())
        if amount <= 0:
            raise ValueError
        print(f"✅ 금액: {amount}\n")
    except ValueError:
        print("❌ 금액은 0보다 큰 유효한 정수로 입력해야 합니다.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n❌ 사용자가 취소했습니다.")
        sys.exit(0)
        
    # 3. 기준 날짜 입력
    start_date_input = input("기준 날짜 입력 (예: 2025-10-01): ").strip()
    
    if not start_date_input:
        print("❌ 날짜를 입력해야 합니다.")
        sys.exit(1)
    
    try:
        # 날짜 형식 검증
        datetime.strptime(start_date_input, "%Y-%m-%d")
        print(f"✅ 기준 날짜: {start_date_input} (시간은 02:00:00 +09 KST로 설정됩니다)\n")
    except ValueError:
        print("❌ 올바른 날짜 형식이 아닙니다. (예: 2025-10-01)")
        sys.exit(1)

    # 4. 최종 확인
    print("="*60)
    print("다음 내용으로 잔고를 설정합니다:")
    print(f"  - 계좌: {account_name}")
    print(f"  - 금액: {amount}")
    print(f"  - 날짜: {start_date_input} 02:00:00 +09")
    print("="*60)
    confirm = input("\n진행하시겠습니까? (yes/y): ").strip().lower()
    
    if confirm in ['yes', 'y']:
        success = update_account_balance_direct(account_name, amount, start_date_input)
        if success:
            print("\n✅ 모든 작업이 완료되었습니다!")
        else:
            print("\n❌ 작업 중 오류가 발생했습니다.")
            sys.exit(1)
    else:
        print("\n❌ 작업이 취소되었습니다.")
        sys.exit(0)
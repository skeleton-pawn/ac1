"""
PG Ledger: 지정된 bank.* 계좌와 쌍이 되는 liquidity.* 계좌 및 관련 거래 기록을 물리적으로 삭제하는 관리자 전용 스크립트.
"""
import psycopg
import sys

# 데이터베이스 연결 설정 (main1.py와 동일)
DB_CONFIG = {
    'dbname': 'pgledger',
    'user': 'pgledger',
    'password': 'pgledger',
    'host': 'localhost',
    'port': 5432
}

def get_account_id(cur, account_name):
    """계정 이름으로 ID를 조회합니다."""
    # pgledger_accounts_view는 ID를 조회하는 데 사용됩니다.
    cur.execute(
        "SELECT id FROM pgledger_accounts_view WHERE name = %s", 
        (account_name,)
    )
    result = cur.fetchone()
    if result:
        return result[0]
    return None

def get_bank_accounts_with_zero_balance():
    """잔고 상태와 관계없이 'bank.' 계정 목록을 조회합니다."""
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)
        cur = conn.cursor()
        # 모든 'bank.' 계좌를 조회
        cur.execute("""
            SELECT name, balance 
            FROM pgledger_accounts_view 
            WHERE name LIKE 'bank.%'
            ORDER BY name
        """)
        return cur.fetchall()
    except psycopg.Error as e:
        print(f"❌ 데이터베이스 오류 발생: 계좌 목록을 불러올 수 없습니다. {e}")
        return []
    finally:
        if conn:
            conn.close()

def delete_account_pair(bank_account_name):
    """
    지정된 bank 계좌와 liquidity 쌍 계좌 및 관련 거래를 삭제합니다.
    (관리자 권한 필요)
    """
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)
        conn.autocommit = False # 트랜잭션 수동 관리
        cur = conn.cursor()

        # 1. 쌍 계정 이름 설정 및 ID 조회
        if not bank_account_name.startswith("bank."):
            print("❌ 오류: 계정 이름이 'bank.'로 시작하지 않아 처리를 중단합니다.")
            return False

        liquidity_account_name = "liquidity" + bank_account_name[4:] 
        
        bank_id = get_account_id(cur, bank_account_name)
        liquidity_id = get_account_id(cur, liquidity_account_name)

        if not bank_id or not liquidity_id:
            print(f"❌ 오류: 계정 ID를 찾을 수 없습니다. (bank: {bank_id}, liquidity: {liquidity_id})")
            print("  -> 계정 이름이 정확한지, 이미 삭제되지는 않았는지 확인하세요.")
            return False

        account_ids_to_delete = [bank_id, liquidity_id]
        print(f"✅ 삭제 대상 계정 ID: {bank_account_name} ({bank_id}), {liquidity_account_name} ({liquidity_id})")

        # 2. 관련 거래 기록 (Entries) 삭제
        # pgledger는 거래 기록이 있는 계정을 삭제하지 못하므로, 관리자 권한으로 삭제합니다.
        print("\n⏳ 1단계: 관련 거래 기록 (Entries) 삭제 중...")
        
        # pgledger_entries 테이블에서 해당 계정 ID를 참조하는 모든 기록의 transfer_id 조회
        cur.execute(
            """
            SELECT DISTINCT transfer_id 
            FROM pgledger_entries 
            WHERE account_id = ANY(%s);
            """,
            (account_ids_to_delete,)
        )
        transfer_ids = [row[0] for row in cur.fetchall()]
        
        # Entries 삭제
        cur.execute(
            """
            DELETE FROM pgledger_entries 
            WHERE account_id = ANY(%s);
            """,
            (account_ids_to_delete,)
        )
        deleted_count = cur.rowcount
        print(f"  -> 총 {deleted_count}개의 Entries가 삭제되었습니다.")
        
        # 삭제된 Entries에 연결된 Transfers도 삭제 (무결성 유지)
        if transfer_ids:
            cur.execute(
                """
                DELETE FROM pgledger_transfers 
                WHERE id = ANY(%s);
                """,
                (transfer_ids,)
            )
            print(f"  -> Entries에 연결된 총 {len(transfer_ids)}개의 Transfers가 삭제되었습니다.")


        # 3. 계정 레코드 (Accounts) 삭제
        print("\n⏳ 2단계: 계정 레코드 (Accounts) 삭제 중...")
        
        # pgledger_accounts 테이블에서 두 계정 모두 삭제
        cur.execute(
            """
            DELETE FROM pgledger_accounts 
            WHERE id = ANY(%s)
            RETURNING name;
            """,
            (account_ids_to_delete,)
        )
        deleted_accounts = cur.fetchall()
        print(f"  -> 계정 {', '.join([row[0] for row in deleted_accounts])}가 삭제되었습니다.")

        # 4. 커밋 및 완료
        conn.commit()
        print("\n🎉 성공: 계좌 쌍 및 모든 관련 거래가 완전히 삭제되었습니다. (DB 커밋 완료)")
        return True

    except psycopg.Error as e:
        if conn:
            conn.rollback()
        print(f"❌ 데이터베이스 트랜잭션 오류 발생 (롤백됨): {e}")
        return False
    except Exception as e:
        print(f"❌ 일반 오류 발생: {e}")
        return False
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    print("\n" + "="*70)
    print("      PG Ledger 관리자: 계좌 쌍 및 거래 기록 물리적 삭제 스크립트")
    print("="*70)
    
    accounts_info = get_bank_accounts_with_zero_balance()
    
    if not accounts_info:
        print("🚨 삭제 가능한 'bank.' 계좌가 데이터베이스에 없습니다.")
        print("   -> (잔고 상태와 관계없이 목록에 표시되지만, 잔고가 있다면 삭제가 실패할 수 있습니다.)")
        sys.exit(1)
        
    print("\n--- 1. 삭제할 'bank.' 계좌 선택 ---")
    account_map = {}
    for i, (name, balance) in enumerate(accounts_info):
        account_map[str(i + 1)] = name
        print(f"{i+1}. {name} (현재 잔고: {balance})")
        
    account_name_to_delete = None
    while True:
        choice = input("\n삭제할 계좌 번호 선택 (9: 종료): ").strip()
        if choice == '9':
            print("스크립트를 종료합니다.")
            sys.exit(0)
            
        if choice in account_map:
            account_name_to_delete = account_map[choice]
            break
        else:
            print("❗ 잘못된 번호입니다. 목록 내의 번호를 선택해 주세요.")

    # 최종 확인
    print(f"\n[경고]: {account_name_to_delete} 계좌와 쌍이 되는 liquidity 계좌, 그리고")
    print("        이와 관련된 모든 거래 기록이 영구적으로 삭제됩니다.")
    
    confirm = input("정말로 삭제를 진행하시겠습니까? (yes = 'y'): ").strip().lower()

    if confirm == 'y':
        delete_account_pair(account_name_to_delete)
    else:
        print("삭제 작업이 취소되었습니다.")
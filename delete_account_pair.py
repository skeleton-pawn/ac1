"""
PG Ledger: 지정된 계좌와 쌍이 되는 liquidity.* 계좌 및 관련 거래 기록을 물리적으로 삭제하는 관리자 전용 스크립트.
"""
import psycopg
import sys

# 데이터베이스 연결 설정
DB_CONFIG = {
    'dbname': 'pgledger',
    'user': 'pgledger',
    'password': 'pgledger',
    'host': 'localhost',
    'port': 5432
}

def get_account_id(cur, account_name):
    """계정 이름으로 ID를 조회합니다."""
    cur.execute(
        "SELECT id FROM pgledger_accounts_view WHERE name = %s", 
        (account_name,)
    )
    result = cur.fetchone()
    if result:
        return result[0]
    return None

def get_accounts_by_prefix(prefix):
    """특정 접두사로 시작하는 계정 목록을 조회합니다."""
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT name, balance 
            FROM pgledger_accounts_view 
            WHERE name LIKE %s
            ORDER BY name
        """, (f"{prefix}.%",))
        return cur.fetchall()
    except psycopg.Error as e:
        print(f"❌ 데이터베이스 오류 발생: 계좌 목록을 불러올 수 없습니다. {e}")
        return []
    finally:
        if conn:
            conn.close()

def delete_account_pair(account_name, prefix):
    """
    지정된 계좌와 liquidity 쌍 계좌 및 관련 거래를 삭제합니다.
    (관리자 권한 필요)
    """
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        # 1. 쌍 계정 이름 설정 및 ID 조회
        if not account_name.startswith(f"{prefix}."):
            print(f"❌ 오류: 계정 이름이 '{prefix}.'로 시작하지 않아 처리를 중단합니다.")
            return False

        # prefix 부분을 liquidity로 교체
        liquidity_account_name = "liquidity" + account_name[len(prefix):]
        
        account_id = get_account_id(cur, account_name)
        liquidity_id = get_account_id(cur, liquidity_account_name)

        if not account_id or not liquidity_id:
            print(f"❌ 오류: 계정 ID를 찾을 수 없습니다. ({prefix}: {account_id}, liquidity: {liquidity_id})")
            print("  -> 계정 이름이 정확한지, 이미 삭제되지는 않았는지 확인하세요.")
            return False

        account_ids_to_delete = [account_id, liquidity_id]
        print(f"✅ 삭제 대상 계정 ID: {account_name} ({account_id}), {liquidity_account_name} ({liquidity_id})")

        # 2. 관련 거래 기록 (Entries) 삭제
        print("\n⏳ 1단계: 관련 거래 기록 (Entries) 삭제 중...")
        
        # transfer_id 조회
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
        
        # Transfers 삭제
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

def select_and_delete_account(prefix, account_type_name):
    """계정을 선택하고 삭제하는 프로세스를 처리합니다."""
    accounts_info = get_accounts_by_prefix(prefix)
    
    if not accounts_info:
        print(f"\n🚨 삭제 가능한 '{prefix}.' 계좌가 데이터베이스에 없습니다.")
        input("\n아무 키나 눌러 메인 메뉴로 돌아가기...")
        return
        
    print(f"\n--- {account_type_name} 계좌 목록 ---")
    account_map = {}
    for i, (name, balance) in enumerate(accounts_info):
        account_map[str(i + 1)] = name
        print(f"{i+1}. {name} (현재 잔고: {balance})")
    print("0. 메인 메뉴로 돌아가기")
        
    account_name_to_delete = None
    while True:
        choice = input(f"\n삭제할 계좌 번호 선택: ").strip()
        
        if choice == '0':
            return
            
        if choice in account_map:
            account_name_to_delete = account_map[choice]
            break
        else:
            print("❗ 잘못된 번호입니다. 목록 내의 번호를 선택해 주세요.")

    # 최종 확인
    print(f"\n{'='*70}")
    print(f"[경고]: {account_name_to_delete} 계좌와 쌍이 되는 liquidity 계좌, 그리고")
    print("        이와 관련된 모든 거래 기록이 영구적으로 삭제됩니다.")
    print(f"{'='*70}")
    
    confirm = input("정말로 삭제를 진행하시겠습니까? (yes/y): ").strip().lower()

    if confirm == 'yes' or confirm == 'y':
        delete_account_pair(account_name_to_delete, prefix)
    else:
        print("\n❌ 삭제 작업이 취소되었습니다.")
    
    input("\n아무 키나 눌러 메인 메뉴로 돌아가기...")

def show_main_menu():
    """메인 메뉴를 표시합니다."""
    print("\n" + "="*70)
    print("      PG Ledger 관리자: 계좌 쌍 및 거래 기록 물리적 삭제 스크립트")
    print("="*70)
    print("\n[메뉴]")
    print("1. 삭제할 은행 계좌 조회 (bank.*)")
    print("2. 삭제할 증권 계좌 조회 (stock.*)")
    print("9. 종료")
    print("="*70)

def main():
    """메인 프로그램 루프"""
    while True:
        show_main_menu()
        choice = input("\n메뉴 선택: ").strip()
        
        if choice == '1':
            select_and_delete_account('bank', '은행')
        elif choice == '2':
            select_and_delete_account('stock', '증권')
        elif choice == '9':
            print("\n👋 스크립트를 종료합니다.")
            sys.exit(0)
        else:
            print("\n❗ 잘못된 선택입니다. 1, 2, 또는 9를 입력하세요.")
            input("\n아무 키나 눌러 계속...")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 사용자가 종료했습니다.")
        sys.exit(0)
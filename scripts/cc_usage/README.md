# cc-usage 실행 방법

## 1) 설치

```bash
cd /Users/ijinseong/Documents/golf/dy_golfcart_monitoring
python3 -m venv .venv-cc-usage
source .venv-cc-usage/bin/activate
pip install -r scripts/cc_usage/requirements.txt
```

## 2) 환경 변수 설정

```bash
export DATABASE_URL='<YOUR_DATABASE_URL>'
```

## 3) DB 초기화

```bash
python3 scripts/cc_usage/main.py init-db
```

## 4) Claude 실행 래퍼 등록(자동 시작/종료)

```bash
chmod +x /Users/ijinseong/Documents/golf/dy_golfcart_monitoring/scripts/cc_usage/cc-claude
echo "alias claude='/Users/ijinseong/Documents/golf/dy_golfcart_monitoring/scripts/cc_usage/cc-claude'" >> ~/.zshrc
source ~/.zshrc
```

## 5) 상태 확인

```bash
python3 scripts/cc_usage/main.py status --format plain
```

`claude` 실행 시:
- 첫 세션 시작: `cc-usage daemon` 자동 시작
- 마지막 세션 종료: `cc-usage daemon` 자동 종료

## 6) tmux 하단 상태바 표시

`~/.tmux.conf`:

```tmux
set -g status on
set -g status-position bottom
set -g status-interval 1
set -g status-right '#(/usr/bin/env DATABASE_URL=$DATABASE_URL python3 /Users/ijinseong/Documents/golf/dy_golfcart_monitoring/scripts/cc_usage/main.py status --format tmux) | %H:%M'
```

적용:

```bash
tmux source-file ~/.tmux.conf
tmux new -As main
```

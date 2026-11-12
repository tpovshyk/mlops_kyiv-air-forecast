"""Відтворення зареєстрованої моделі з її ідентифікаторів.

1. Бере з реєстру MLflow версію моделі, її теги git_commit і data_hash та MAE запуску.
2. Відновлює код цього коміту в тимчасовій папці (git worktree).
3. Завантажує з DVC-remote саме той знімок ознак і перевіряє, що його хеш = data_hash.
4. Навчає заново й порівнює MAE з оригіналом у межах допуску.

Запуск: make reproduce (версія champion) або make reproduce VERSION=1
"""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from kyiv_air import settings
from kyiv_air.train import MODEL_NAME, SNAPSHOT_DVC, dvc_hash

DEFAULT_TOLERANCE = 0.01  # 1% відносної різниці MAE


def run(cmd: list[str], cwd: Path, env: dict | None = None) -> None:
    print("$", " ".join(cmd))
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--version")
    g.add_argument("--alias")
    p.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    args = p.parse_args()

    from mlflow import MlflowClient

    client = MlflowClient()
    mv = (client.get_model_version(MODEL_NAME, args.version) if args.version
          else client.get_model_version_by_alias(MODEL_NAME, args.alias))
    commit, data_hash = mv.tags["git_commit"], mv.tags["data_hash"]
    reference = client.get_run(mv.run_id).data.metrics["model_mae"]
    print(f"Версія {mv.version}: git_commit={commit[:8]} data_hash={data_hash} MAE={reference:.4f}")

    # resolve(): на macOS /var — це посилання на /private/var, і DVC плутається в шляхах.
    workdir = Path(tempfile.mkdtemp(prefix="kyiv_air_repro_")).resolve() / "repo"
    env = {**os.environ,
           "AWS_ACCESS_KEY_ID": os.environ["MINIO_ROOT_USER"],
           "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_ROOT_PASSWORD"],
           "PYTHONPATH": str(workdir / "src")}
    env.pop("VIRTUAL_ENV", None)
    try:
        run(["git", "worktree", "add", "--detach", str(workdir), commit], settings.ROOT)
        rel_dvc = SNAPSHOT_DVC.relative_to(settings.ROOT)
        if dvc_hash(workdir / rel_dvc) != data_hash:
            raise SystemExit(f"У коміті {commit[:8]} інший знімок даних, ніж у тезі data_hash")
        run(["uv", "run", "--frozen", "dvc", "pull", str(rel_dvc)], workdir, env)
        if not (workdir / rel_dvc.with_suffix("")).exists():
            raise SystemExit("Після dvc pull знімка немає: перевір, що MinIO запущено")
        out = workdir / "repro_metrics.json"
        run(["uv", "run", "--frozen", "python", "-m", "kyiv_air.train",
             "--run-name", f"reproduce-v{mv.version}", "--tag", f"reproduces_version={mv.version}",
             "--metrics-out", str(out)], workdir, env)
        new = json.loads(out.read_text())["model_mae"]
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(workdir)], cwd=settings.ROOT)
        shutil.rmtree(workdir.parent, ignore_errors=True)

    diff = abs(new - reference) / reference
    verdict = "PASS" if diff <= args.tolerance else "FAIL"
    print(f"{verdict}: MAE оригіналу {reference:.4f}, відтворено {new:.4f}, "
          f"різниця {diff:.2%} (допуск {args.tolerance:.0%})")
    if verdict == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

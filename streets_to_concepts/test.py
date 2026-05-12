import sys
from pathlib import Path

WORK_REPO = Path("/opt/lampp/htdocs/saa-nexus-scripts")

sys.path.append(str(WORK_REPO))

print("\nPYTHON:")
print(sys.executable)

print("\nPATH ADDED:")
print(WORK_REPO)

print("\nCONTENTS:")
print(list(WORK_REPO.iterdir()))

print("\nSYS.PATH:")
for p in sys.path:
    print(p)

print("\nTRY IMPORT")

import modules

print("SUCCESS")

sys.path.insert(0, str(WORK_REPO))

print(sys.path[0])

import modules

print("SUCCESS")
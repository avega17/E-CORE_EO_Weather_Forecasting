"""Prepare the isolated, tested Flash dependencies in a Colab runtime."""
from pathlib import Path
import os
import shutil
import subprocess
import sys


def colab_model_python(prefix='/content/ecore-flash-py312'):
    prefix=Path(prefix)
    model_python=prefix/'bin/python'
    check=(
        'import torch, natten, earth2grid\n'
        'from earth2studio.models.nn.stormscope_flash import FlashModel\n'
        'assert torch.cuda.is_available() and callable(FlashModel.from_checkpoint)'
    )
    # The isolated interpreter does not share the notebook's inline backend.
    child_env={**os.environ,'MPLBACKEND':'Agg'}
    if model_python.exists() and subprocess.run([str(model_python),'-c',check],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,env=child_env).returncode==0:
        return str(model_python)
    compiler=subprocess.check_output(['nvcc','--version'],text=True)
    if 'release 13.' not in compiler:
        raise RuntimeError('The documented Flash environment requires the CUDA 13 compiler. Choose a compatible Colab L4 runtime.')
    uv=shutil.which('uv')
    if uv is None:
        subprocess.run([sys.executable,'-m','pip','install','-q','uv'],check=True)
        uv=shutil.which('uv')
    subprocess.run([uv,'venv','--seed','--python','3.12',str(prefix)],check=True)
    pip=[str(model_python),'-m','pip','install','-q']
    print('Installing the isolated Flash environment for Python 3.12.',flush=True)
    subprocess.run(pip+['--extra-index-url','https://download.pytorch.org/whl/cu130',
        '--find-links','https://whl.natten.org','torch==2.13.0+cu130','natten==0.21.7+torch2130cu130',
        'nvidia-physicsnemo==2.2.2','earth2studio==0.20.0','rasterio','zarr==3.3.0','ninja',
        'huggingface_hub','pyproj','setuptools==77.0.3','wheel','matplotlib','scipy'],check=True)
    capability=subprocess.check_output([str(model_python),'-c','import torch; print(".".join(map(str,torch.cuda.get_device_capability())))'],text=True).strip()
    print('Building Earth2Grid CUDA support. This can take several minutes.',flush=True)
    subprocess.run(pip+['--no-deps','--no-build-isolation',
        'https://github.com/NVlabs/earth2grid/archive/main.tar.gz'],check=True,
        env={**child_env,'MAX_JOBS':'2','TORCH_CUDA_ARCH_LIST':capability})
    subprocess.run([str(model_python),'-c',check],check=True,env=child_env)
    return str(model_python)

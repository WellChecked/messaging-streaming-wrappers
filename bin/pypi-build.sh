if [ -d dist ]
then
  rm dist/*.tar.gz
  rm dist/*.whl
fi

python3 -m build
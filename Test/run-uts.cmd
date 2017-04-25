for /f %%i in (%~dp0\parallel-tests.txt) do (
  del %%i.results.xml
)

for /f %%i in (%~dp0\serial-tests.txt) do (
  del %%i.results.xml
)

for /f %%i in (%~dp0\parallel-tests.txt) do (
  del %%i.results.xml
  start "%%i" ..\..\packages\NUnit.ConsoleRunner.3.6.1\tools\nunit3-console.exe Microsoft.Azure.Toolkit.Replication.Test.dll --test=%%i --out=%%i.results.xml
)

for /f %%i in (%~dp0\serial-tests.txt) do (
  del %%i.results.xml
  start "%%i" /wait ..\..\packages\NUnit.ConsoleRunner.3.6.1\tools\nunit3-console.exe Microsoft.Azure.Toolkit.Replication.Test.dll --test=%%i --out=%%i.results.xml
)

$PyVers="36","37","38","39"

[System.Collections.ArrayList]$PyVersFull = $PyVers
foreach ($Ver in $PyVers)
{
	[Void]$PyVersFull.Add("$Ver-32")
}

foreach ($Ver in $PyVersFull)
{
	& "$env:LOCALAPPDATA\Programs\Python\Python$Ver\python.exe" -m venv epy$Ver
	
	. ".\epy$Ver\Scripts\Activate.ps1"
	pip install -e .
	pip install wheel
	pip wheel . --no-deps -w distr
}


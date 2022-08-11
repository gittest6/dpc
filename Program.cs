// global using Microsoft.Data.SqlClient;
global using System;
global using System.Collections.Generic;
global using System.Data;
global using System.Data.SqlClient;
global using System.Diagnostics;
global using System.IO;
global using System.Linq;
global using System.Net.NetworkInformation;
global using System.Security;
global using System.Threading.Channels;
global using System.Threading.Tasks;
using DpcNs;

// AppContext.SetSwitch ("Switch.Microsoft.Data.SqlClient.SuppressInsecureTLSWarning", true);

string processName = System.Diagnostics.Process.GetCurrentProcess ().ProcessName;
string appDirPath = Path.Combine (
	Environment.GetFolderPath (Environment.SpecialFolder.CommonApplicationData),
	processName);
if (! Directory.Exists (appDirPath))
	Directory.CreateDirectory (appDirPath);

using StreamWriter logWriter = File.AppendText (processName + ".log");
logWriter.AutoFlush = true;
string ioConnStr = args[0];

try
{
	var dpc = await Dpc.createAsync (ioConnStr, logWriter);
	await dpc.runRequestLoop ();
}
catch (Exception e)
{
	await logWriter.writeLineWithTimeAsync (e.ToString ());
}

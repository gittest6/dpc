namespace DpcNs;

public static class Extensions
{
	public static Task writeLineWithTimeAsync (this StreamWriter writer,
		string msg) => writer.WriteLineAsync (
			DateTime.Now.ToString ("s") + "\t" + msg);
}

namespace DpcNs;

class Dpc
{
	string ioConnStr;
	
	SqlConnection ioSqlConn;
	StreamWriter logWriter;
	SqlCommand ioSqlCmd;
	DataTable requestTable = new (), logTable = new (), listsTable = new (),
		paramTable = new ();
	string pointConnStrTpl;
	
	public static Task<Dpc> createAsync (string ioConnStr, StreamWriter logWriter)
	{
		Dpc dpc = new (ioConnStr, logWriter);
		return dpc.initializeAsync ();
	}
	
	Dpc (string ioConnStr, StreamWriter logWriter)
	{
		ioSqlConn = new SqlConnection (ioConnStr);
		this.logWriter = logWriter;
		ioSqlCmd = ioSqlConn.CreateCommand ();
		pointConnStrTpl = string.Empty;
		this.ioConnStr = ioConnStr;
	}
	
	async Task<Dpc> initializeAsync ()
	{
		await checkConnectionOpen ();
		ioSqlCmd.CommandText = "select pointConnStrTpl from settings;";
		object? val = null;
		while (val == null)
		{
			try
			{
				val = await ioSqlCmd.ExecuteScalarAsync ();
			}
			catch (Exception e)
			{
				await logWriter.writeLineWithTimeAsync (e.ToString ());
				await Task.Delay (60000);
			}
		}
		pointConnStrTpl = (string) val;
		return this;
	}

	async Task checkConnectionOpen ()
	{
		while (ioSqlConn.State != ConnectionState.Open)
		{
			try
			{
				await ioSqlConn.OpenAsync ();
			}
			catch (Exception e)
			{
				await logWriter.writeLineWithTimeAsync (e.ToString ());
				await Task.Delay (60000);
			}
		}
	}
	
	public async Task runRequestLoop ()
	{
		bool firstLoad = true;
		for (;;)
		{
			try
			{
				ioSqlCmd.CommandText = "exec getRequestData;";
				SqlDataReader sqlReader = await ioSqlCmd.ExecuteReaderAsync ();
				await using (sqlReader)
				{
					if (sqlReader.HasRows)
					{
						requestTable.Load (sqlReader);
						paramTable.Load (sqlReader);
						logTable.Load (sqlReader);
						listsTable.Load (sqlReader);
						
						if (firstLoad)
						{
							var reqIdCol = requestTable.Columns["reqId"]
								?? throw new Exception ("null");
							requestTable.PrimaryKey = new DataColumn[] { reqIdCol };
							var logRowIdCol = logTable.Columns["logRowId"]
								?? throw new Exception ("null");
							logTable.PrimaryKey = new DataColumn[] { logRowIdCol };
							firstLoad = false;
						}
						
						await processRequests ();
						
						requestTable.Clear ();
						paramTable.Clear ();
						logTable.Clear ();
						listsTable.Clear ();
					}
				}
			}
			catch (Exception e)
			{
				await logWriter.writeLineWithTimeAsync (e.ToString ());
				await checkConnectionOpen ();
			}
			await Task.Delay (60000);
		}
	}
	
	async Task processRequests ()
	{
		var requestCount = requestTable.Rows.Count;
		var reqOutDict = new Dictionary<int, OutData> (requestCount);
		var paramDict = new Dictionary<int, IEnumerable<SqlParamData>> (requestCount);
		for (int i = 0; i < requestCount; i++)
		{
			requestTable.Rows[i]["dtStart"] = DateTime.Now;
			
			var reqId = (int) requestTable.Rows[i]["reqId"];
			
			reqOutDict[reqId] = new OutData (
				Channel.CreateUnbounded<DataTable> (),
				i > 0 ? new SqlConnection (ioConnStr) : ioSqlConn,
				DBNull.Value);
				
			var paramRows = paramTable.Select ("reqId = " + reqId);
			if (paramRows.Length > 0)
				paramDict[reqId] = paramRows.Select (row =>
					new SqlParamData ((string) row["name"],
						(SqlDbType) row["typeId"], row["value"]));
		}
		
		var points = await getPoints ();
		var srcTask = Parallel.ForEachAsync (points,
			new ParallelOptions { MaxDegreeOfParallelism = 15 },
			async (point, token) =>
		{
			var reqLogDict = point.reqLogDict;
			
			try
			{
				await using var sqlConn = point.sqlConn;
				checkPing (point.addr);
				await sqlConn.OpenAsync ();
				await using SqlCommand sqlCmd = sqlConn.CreateCommand ();
				sqlCmd.CommandTimeout = 60 * 5;

				foreach (var reqId in reqLogDict.Keys)
				{
					reqLogDict[reqId].dtStart = DateTime.Now;
					var requestRow = requestTable.Rows.Find (reqId)
						?? throw new Exception ("null");
					sqlCmd.CommandText = (string) requestRow["text"];
					if (paramDict.ContainsKey (reqId))
						sqlCmd.Parameters.AddRange (paramDict[reqId].Select (
							pd => new SqlParameter (
									pd.ParameterName, pd.SqlDbType)
								{ Value = pd.Value }).ToArray ());
					var resultTable = new DataTable ();
					try
					{
						await using SqlDataReader sqlReader = await sqlCmd.ExecuteReaderAsync (
							CommandBehavior.SingleResult);
						resultTable.Load (sqlReader);
					}
					catch (Exception e)
					{
						reqLogDict[reqId].errMsg = e.Message;
					}
					if (paramDict.ContainsKey (reqId))
						sqlCmd.Parameters.Clear ();
					resultTable.Columns.Add ("logRowId", typeof (long),
						reqLogDict[reqId].logRowId.ToString ()).SetOrdinal (0);
					resultTable.Columns.Add ("id", typeof (long),
						"null").SetOrdinal (0);
					await reqOutDict[reqId].ch.Writer.WriteAsync (resultTable);
					reqLogDict[reqId].dtFinish = DateTime.Now;
				}
			}
			catch (Exception e)
			{
				foreach (var reqId in reqLogDict.Keys)
				{
					reqLogDict[reqId].dtStart = reqLogDict[reqId].dtFinish =
						DateTime.Now;
					reqLogDict[reqId].errMsg = e.Message;
				}
			}
		}).ContinueWith (t => {
			foreach (var outData in reqOutDict.Values)
				outData.ch.Writer.Complete ();
		});
		
		var outTask = Parallel.ForEachAsync (reqOutDict.Keys, async (reqId, token) =>
		{
			var outData = reqOutDict[reqId];
			var sqlConn = outData.sqlConn;
			try
			{
				if (sqlConn != ioSqlConn)
					await sqlConn.OpenAsync ();
				
				using SqlBulkCopy sbc = new (sqlConn);
				var requestRow = requestTable.Rows.Find (reqId)
					?? throw new Exception ("null");
				sbc.DestinationTableName = (string) requestRow["dstTable"];
				await foreach (var table in outData.ch.Reader.ReadAllAsync ())
					await sbc.WriteToServerAsync (table);
			}
			catch (Exception e)
			{
				reqOutDict[reqId].errMsg = e.ToString ();
			}
			finally
			{
				if (sqlConn != ioSqlConn)
					await sqlConn.DisposeAsync ();
			}
		});
		
		await Task.WhenAll (srcTask, outTask);

		foreach (var point in points)
			foreach (var logEntry in point.reqLogDict.Values)
			{
				var logRow = logTable.Rows.Find (logEntry.logRowId)
					?? throw new Exception ("null");
				logRow["dtStart"] = logEntry.dtStart;
				logRow["dtFinish"] = logEntry.dtFinish;
				logRow["errMsg"] = logEntry.errMsg;
			}

		foreach (var reqId in reqOutDict.Keys)
		{
			var requestRow = requestTable.Rows.Find (reqId)
				?? throw new Exception ("null");
			requestRow["dtFinish"] = DateTime.Now;
			requestRow["errMsg"] = reqOutDict[reqId].errMsg;
		}

		SqlDataAdapter sqladapter;
		
		ioSqlCmd.CommandText = "select logRowId, reqId, pointId, dtStart, dtFinish, errMsg from log;";
		sqladapter = new SqlDataAdapter (ioSqlCmd);
		new SqlCommandBuilder (sqladapter);
		sqladapter.Update (logTable);

		ioSqlCmd.CommandText = "select reqId, dtStart, dtFinish, errMsg from requests;";
		sqladapter = new SqlDataAdapter (ioSqlCmd);
		new SqlCommandBuilder (sqladapter);
		sqladapter.Update (requestTable);
	}

	async Task<IEnumerable<Point>> getPoints ()
	{
		var pointDict = new Dictionary<string, Point> ();
		HashSet<string>? pointFilter = logTable.Rows.Count == 0 ? null :
			logTable.Rows.Cast<DataRow> ().Select (row => (string) row["pointId"])
				.ToHashSet ();
		fillPointDict (pointDict, pointFilter);

		if (pointFilter == null)
		{
			var rpIdPairs = requestTable.Rows.Cast<DataRow> ().Join (
				pointDict.Keys, requestRow => true, pointId => true,
				(requestRow, pointId) =>
					"(" + requestRow["reqId"] + ", '" + pointId + "')");
			const int MSSQL_INSERT_VALUES_LIMIT = 1000;
			foreach (var rpIdPairsCh in rpIdPairs.Chunk (MSSQL_INSERT_VALUES_LIMIT))
			{
				string sqlGetLogRows = "insert into log (reqId, pointId) output inserted.* values " +
					string.Join (",", rpIdPairsCh) + ";";
				ioSqlCmd.CommandText = sqlGetLogRows;
				SqlDataReader sqlReader = await ioSqlCmd.ExecuteReaderAsync ();
				await using (sqlReader)
				{
					logTable.Load (sqlReader);
				}
			}
		}
		else
		{
			var missingPointLogRows = logTable.Rows.Cast<DataRow> ().ExceptBy (
				pointDict.Keys, row => row["pointId"]);
			foreach (var logRow in missingPointLogRows)
			{
				logRow["dtStart"] = logRow["dtFinish"] = DateTime.Now;
				logRow["errMsg"] = "Точка отсутствует в списке.";
			}
		}
		
		var emptyLogRows = logTable.Rows.Cast<DataRow> ().Where (
			logRow => logRow["dtStart"] == DBNull.Value);
		foreach (var logRow in emptyLogRows)
		{
			var pointId = (string) logRow["pointId"];
			var reqId = (int) logRow["reqId"];
			var logRowId = (long) logRow["logRowId"];
			
			pointDict[pointId].reqLogDict[reqId] =
				new LogEntry (logRowId, DBNull.Value, DBNull.Value, DBNull.Value);
		}
		
		return pointDict.Values;
	}

	void fillPointDict (Dictionary<string, Point> pointDict,
		HashSet<string>? pointFilter)
	{
		var requestCount = requestTable.Rows.Count;

		foreach (var listRow in listsTable.Rows.Cast<DataRow> ())
		{
			SecureString secStrPassword = new ();
			foreach (char c in (string) listRow["password"])
				secStrPassword.AppendChar (c);
			secStrPassword.MakeReadOnly ();
			SqlCredential sqlcred = new ((string) listRow["login"], secStrPassword);

			var recColl = File.ReadLines ((string) listRow["path"]).Select (
				line => line.Split ('\t'));
			foreach (var rec in recColl)
				if (pointFilter == null || pointFilter.Contains (rec[0]))
					pointDict[rec[0]] = new Point (rec[1],
						new SqlConnection (
							string.Format (pointConnStrTpl, rec[1]), sqlcred),
						new Dictionary<int, LogEntry> (requestCount));
		}
	}

	static void checkPing (string addr)
	{
		Ping ping = new ();
		IPStatus pingStatus = IPStatus.Unknown;
		for (int i = 0; i < 3; i++)
		{
			pingStatus = ping.Send (addr).Status;
			if (pingStatus == IPStatus.Success)
				break;
		}
		if (pingStatus != IPStatus.Success)
			throw new Exception (pingStatus.ToString ());
	}
}

record Point (string addr, SqlConnection sqlConn, Dictionary<int, LogEntry> reqLogDict);
record SqlParamData (string ParameterName, SqlDbType SqlDbType, object Value);
record LogEntry (long logRowId, object dtStart, object dtFinish, object errMsg)
{
	public long logRowId { get; init; } = logRowId;
	public object dtStart { get; set; } = dtStart;
	public object dtFinish { get; set; } = dtFinish;
	public object errMsg { get; set; } = errMsg;
}
record OutData (Channel<DataTable> ch, SqlConnection sqlConn, object errMsg)
{
	public Channel<DataTable> ch { get; init; } = ch;
	public SqlConnection sqlConn { get; init; } = sqlConn;
	public object errMsg { get; set; } = errMsg;
}

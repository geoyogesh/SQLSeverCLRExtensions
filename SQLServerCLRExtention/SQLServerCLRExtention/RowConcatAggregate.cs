using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Text;

[Serializable]
[Microsoft.SqlServer.Server.SqlUserDefinedAggregate(Format.UserDefined, //use clr serialization to serialize the intermediate result
        IsInvariantToNulls = true, //optimizer property
        IsInvariantToDuplicates = false, //optimizer property
        IsInvariantToOrder = false, //optimizer property
        MaxByteSize = -1)]

public struct ConcatenateRow : IBinarySerialize
{
    public void Init()
    {
        this.intermediateResult = new StringBuilder();
    }

    public void Accumulate(SqlString Value)
    {
        if (Value.IsNull)
        {
            return;
        }

        this.intermediateResult.Append(Value.Value).Append(',');
    }

    public void Merge(ConcatenateRow Group)
    {
        this.intermediateResult.Append(Group.intermediateResult);
    }

    public SqlString Terminate ()
    {
        string output = string.Empty;
        //delete the trailing comma, if any
        if (this.intermediateResult != null
            && this.intermediateResult.Length > 0)
        {
            output = this.intermediateResult.ToString(0, this.intermediateResult.Length - 1);
        }
        return new SqlString(output);
    }

    /// <summary>
    /// The variable that holds the intermediate result of the concatenation
    /// </summary>
    private StringBuilder intermediateResult;

    public void Read(System.IO.BinaryReader r)
    {
        intermediateResult = new StringBuilder(r.ReadString());
    }

    public void Write(System.IO.BinaryWriter w)
    {
        w.Write(this.intermediateResult.ToString());
    }
}

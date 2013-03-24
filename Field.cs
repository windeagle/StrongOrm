using System;
using System.Data;

namespace DianPing.BA.Framework.DAL
{
    internal static class Field
    {
        #region const define

// ReSharper disable InconsistentNaming
        private const string NULL_STRING = "";
        private const long NULL_INT64 = 0L;
        private const int NULL_INT32 = 0;
        private const short NULL_INT16 = 0;
        private const float NULL_FLOAT = 0.00F;
        private const Decimal NULL_DECIMAL = 0.00M;
        private static readonly DateTime NULL_DATETIME = new DateTime(0);
// ReSharper restore InconsistentNaming

        #endregion

        #region 基于列索引

        public static string GetString(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_STRING : rec.GetString(fldnum);
        }

        public static decimal GetDecimal(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_DECIMAL : rec.GetDecimal(fldnum);
        }

        public static int GetInt(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_INT32 : rec.GetInt32(fldnum);
        }

        public static bool GetBoolean(IDataRecord rec, int fldnum)
        {
            return !rec.IsDBNull(fldnum) && rec.GetBoolean(fldnum);
        }

        public static byte GetByte(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? (byte) 0 : rec.GetByte(fldnum);
        }

        public static DateTime GetDateTime(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_DATETIME : rec.GetDateTime(fldnum);
        }

        public static double GetDouble(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? 0 : rec.GetDouble(fldnum);
        }

        public static float GetFloat(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_FLOAT : rec.GetFloat(fldnum);
        }

        public static Guid GetGuid(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? Guid.Empty : rec.GetGuid(fldnum);
        }

        public static int GetInt32(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_INT32 : rec.GetInt32(fldnum);
        }

        public static Int16 GetInt16(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_INT16 : rec.GetInt16(fldnum);
        }

        public static long GetInt64(IDataRecord rec, int fldnum)
        {
            return rec.IsDBNull(fldnum) ? NULL_INT64 : rec.GetInt64(fldnum);
        }

        public static int GetIntEmpty(IDataRecord rec, string fldname)
        {
            return GetIntEmpty(rec, rec.GetOrdinal(fldname));
        }

        /// <summary>
        ///   为数字化特殊处理，返回空则默认为 -9999
        /// </summary>
        /// <param name="rec"> </param>
        /// <param name="flgnum"> </param>
        /// <returns> </returns>
        public static int GetIntEmpty(IDataRecord rec, int flgnum)
        {
            if (rec.IsDBNull(flgnum)) return -9999;
            return rec.GetInt32(flgnum);
        }

        #endregion

        #region 基于列名称

        public static string GetString(IDataRecord rec, string fldname)
        {
            return GetString(rec, rec.GetOrdinal(fldname));
        }

        public static Decimal GetDecimal(IDataRecord rec, string fldname)
        {
            return GetDecimal(rec, rec.GetOrdinal(fldname));
        }

        public static int GetInt(IDataRecord rec, string fldname)
        {
            return GetInt(rec, rec.GetOrdinal(fldname));
        }

        public static bool GetBoolean(IDataRecord rec, string fldname)
        {
            return GetBoolean(rec, rec.GetOrdinal(fldname));
        }

        public static byte GetByte(IDataRecord rec, string fldname)
        {
            return GetByte(rec, rec.GetOrdinal(fldname));
        }

        public static DateTime GetDateTime(IDataRecord rec, string fldname)
        {
            return GetDateTime(rec, rec.GetOrdinal(fldname));
        }

        public static double GetDouble(IDataRecord rec, string fldname)
        {
            return GetDouble(rec, rec.GetOrdinal(fldname));
        }

        public static float GetFloat(IDataRecord rec, string fldname)
        {
            return GetFloat(rec, rec.GetOrdinal(fldname));
        }

        public static Guid GetGuid(IDataRecord rec, string fldname)
        {
            return GetGuid(rec, rec.GetOrdinal(fldname));
        }

        public static int GetInt32(IDataRecord rec, string fldname)
        {
            return GetInt32(rec, rec.GetOrdinal(fldname));
        }

        public static short GetInt16(IDataRecord rec, string fldname)
        {
            return GetInt16(rec, rec.GetOrdinal(fldname));
        }

        public static long GetInt64(IDataRecord rec, string fldname)
        {
            return GetInt64(rec, rec.GetOrdinal(fldname));
        }

        #endregion

        // 屏蔽构造函数
        //private Field() { }

        public static string GetOutPutParam(IDataParameter param, string defaultValue)
        {
            if (param.Value is DBNull || param.Value == null)
                return defaultValue;
            return param.Value.ToString();
        }

        public static int GetOutPutParam(IDataParameter param, int defaultValue)
        {
            if (param.Value is DBNull || param.Value == null || param.Value == DBNull.Value)
                return defaultValue;
            return (int) param.Value;
        }

        public static long GetOutPutParam(IDataParameter param, long defaultValue)
        {
            long result;
            if (param.Value is DBNull || param.Value == null || param.Value == DBNull.Value)
                return defaultValue;
            if (!long.TryParse(param.Value.ToString(), out result))
                return defaultValue;
            return result;
        }

        public static double GetOutPutParam(IDataParameter param, double defaultValue)
        {
            if (param.Value is DBNull || param.Value == null || param.Value == DBNull.Value)
                return defaultValue;
            return (double) param.Value;
        }

        public static DateTime GetOutPutParam(IDataParameter param)
        {
            if (param.Value is DBNull || param.Value == null)
                return DateTime.MinValue;
            return DateTime.Parse(param.Value.ToString());
        }

        public static int GetReturnPram(IDataParameter param)
        {
            if (param.Value is DBNull || param.Value == null || param.Value == DBNull.Value)
                return -1;
            return (int) param.Value;
        }
    }
}
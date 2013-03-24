using System;
using System.Collections;
using System.Data;

namespace DianPing.BA.Framework.DAL
{
    public sealed class ADOHelperParameterCache
    {
        private static readonly Hashtable ParamCache = Hashtable.Synchronized(new Hashtable());

        //static ADOHelperParameterCache()
        //{
        //}

        internal static IDataParameter[] CloneParameters(IDataParameter[] originalParameters, bool setNull = false)
        {
            var dataParameterArray = new IDataParameter[originalParameters.Length];
            int length = originalParameters.Length;
            for (int index = 0; index < length; ++index)
            {
                dataParameterArray[index] = (IDataParameter) ((ICloneable) originalParameters[index]).Clone();
                if (setNull)
                    dataParameterArray[index].Value = DBNull.Value;
            }
            return dataParameterArray;
        }

        public static void CacheParameterSet(string connectionString, string commandText,
                                             params IDataParameter[] commandParameters)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(commandText))
                throw new ArgumentNullException("commandText");
            string str = connectionString + ":" + commandText;
            ParamCache[str.ToLower()] = commandParameters;
        }

        public static IDataParameter[] GetCachedParameterSet(string connectionString, string commandText)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(commandText))
                throw new ArgumentNullException("commandText");
            string str = connectionString + ":" + commandText;
            var originalParameters = ParamCache[str.ToLower()] as IDataParameter[];
            return originalParameters == null ? null : CloneParameters(originalParameters);
        }
    }
}
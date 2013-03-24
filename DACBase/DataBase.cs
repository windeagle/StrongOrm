namespace DianPing.BA.Framework.DAL.DACBase
{
    /// <summary>
    ///   包装数据库访问信息（如Provider和连接字符串）
    /// </summary>
    public class DataBase
    {
        private static DataBase _instance;

        /// <summary>
        ///   默认数据库访问信息（应由Ioc框架注入）
        /// </summary>
        public static DataBase Instance
        {
            get { return _instance ?? (_instance = new DataBase()); }
            set { _instance = value; }
        }

        /// <summary>
        ///   Provider的别名
        /// </summary>
        public string ProviderAlians { get; set; }

        /// <summary>
        ///   连接字符串的别名
        /// </summary>
        public string ConnAlians { get; set; }

        //TODO：需要添加其他的信息，如超时时间等
    }
}
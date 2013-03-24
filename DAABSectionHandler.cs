using System;
using System.Collections.Generic;
using System.Configuration;
using System.Xml;

namespace DianPing.BA.Framework.DAL
{
    public class DAABSectionHandler :
        IConfigurationSectionHandler
    {
        #region IConfigurationSectionHandler Members

        public object Create(object parent, object configContext, XmlNode section)
        {
            var ht = new Dictionary<string, ProviderAlias>();
            XmlNodeList mappings = section.SelectNodes("daabProvider");
            if (mappings != null)
                foreach (XmlNode xmlNode in mappings)
                {
                    if (xmlNode.Attributes["alias"] == null)
                        throw new Exception(
                            "The 'daabProvider' node must contain an attribute named 'alias' with the alias name for the provider.");
                    if (xmlNode.Attributes["assembly"] == null)
                        throw new Exception(
                            "The 'daabProvider' node must contain an attribute named 'assembly' with the name of the assembly containing the provider.");
                    if (xmlNode.Attributes["type"] == null)
                        throw new Exception(
                            "The 'daabProvider' node must contain an attribute named 'type' with the full name of the type for the provider.");
                    ht.Add(xmlNode.Attributes["alias"].Value.ToLower(),
                           new ProviderAlias(xmlNode.Attributes["assembly"].Value,
                                             xmlNode.Attributes["type"].Value));
                }
            return ht;
        }

        #endregion
    }

    public class ProviderAlias
    {
        private readonly string _assemblyName;
        private readonly string _typeName;

        public ProviderAlias(string assemblyName, string typeName)
        {
            _assemblyName = assemblyName;
            _typeName = typeName;
        }

        public string AssemblyName
        {
            get { return _assemblyName; }
        }

        public string TypeName
        {
            get { return _typeName; }
        }
    }
}
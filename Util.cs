using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.ComponentModel;
using System.Data;
using System.Data.Common;

namespace Dau.Data
{
    public static class Util
    {
        /// <summary>
        /// 오브젝트 데이터 복사
        /// </summary>
        /// <typeparam name="Tfrom"></typeparam>
        /// <typeparam name="Tto"></typeparam>
        /// <param name="FromObj"></param>
        /// <param name="ToObj"></param>
        public static void CopyObject<Tfrom, Tto>(Tfrom FromObj, Tto ToObj)
        {

            var parentProperties = FromObj.GetType().GetProperties();
            var childProperties = ToObj.GetType().GetProperties();

            foreach (var parentProperty in parentProperties)
            {
                foreach (var childProperty in childProperties)
                {
                    if (parentProperty.Name == childProperty.Name && parentProperty.PropertyType == childProperty.PropertyType)
                    {
                        childProperty.SetValue(ToObj, parentProperty.GetValue(FromObj));
                        break;
                    }
                }
            }

        }
        public static void CopyObject<T>(T FromObj, T ToObj)
        {
            CopyObject<T, T>(FromObj, ToObj);
        }

        public static List<string> GetChangedProperties<T>(T FromObj, T ToObj, List<string> CheckProperties = null)
        {
            return GetChangedProperties<T>(FromObj, ToObj, CheckProperties);
        }

        public static List<string> GetChangedProperties<Tfrom, Tto>(Tfrom FromObj, Tto ToObj, List<string> CheckProperties = null)
        {
            var parentProperties = FromObj.GetType().GetProperties();
            var childProperties = ToObj.GetType().GetProperties();
            List<string> diffs = new List<string>();
            foreach (var parentProperty in parentProperties)
            {
                var p_val = parentProperty.GetValue(FromObj);
                foreach (var childProperty in childProperties)
                {
                    bool _isCheckProperty = false;
                    if (CheckProperties == null)
                    {
                        _isCheckProperty = true;
                    }
                    else
                    {
                        foreach (var _property in CheckProperties)
                        {
                            if (_property == parentProperty.Name)
                            {
                                _isCheckProperty = true;
                                break;
                            }
                        }
                    }
                    if (_isCheckProperty == true && parentProperty.Name == childProperty.Name && parentProperty.PropertyType == childProperty.PropertyType)
                    {
                        if (p_val != null)
                        {
                            if (p_val.Equals(childProperty.GetValue(ToObj)) == false)
                            {
                                diffs.Add(parentProperty.Name);
                            }
                        }
                        else
                        {
                            if (childProperty.GetValue(ToObj) != null)
                            {
                                diffs.Add(parentProperty.Name);
                            }
                        }
                        break;
                    }
                }
            }
            return diffs;
        }

        public static System.Data.DataTable ToDataTable<T>(this IEnumerable<T> data)
        {
            PropertyDescriptorCollection properties =
                TypeDescriptor.GetProperties(typeof(T));
            System.Data.DataTable table = new System.Data.DataTable();
            
            foreach (PropertyDescriptor prop in properties)
                table.Columns.Add(prop.Name,
                    Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            foreach (T item in data)
            {
                DataRow row = table.NewRow();
                foreach (PropertyDescriptor prop in properties)
                    row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                table.Rows.Add(row);
            }
            return table;
        }
    }
}

using BusComun;
using Comun;
using System;


namespace BusInchost
{
    class RelacionesDalc : cBase
    {
        public RelacionesDalc(cConexion pconn)
        {
            conn = pconn;
        }

        internal bool VaciarRobcl()
        {
            int nRet;
            String sSql;

            sSql = $@"DELETE FROM ROBCL PR
WHERE EXISTS(SELECT 1 FROM IN_RCYO TIN WHERE TIN.OCFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND PR.OCOBLIG = TIN.OCOBLIG ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarRobcl", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRURROBCL", "Error vaciar Robcl ");
                return false;
            }
        }

        internal bool CargarRobcl()
        {
            int nRet;
            String sSql;
            sSql = $@"INSERT INTO ROBCL (OCOBLIG, OCRAIZ, OCFIGURA) 
             SELECT tin.OCOBLIG, tin.OCRAIZ, 'T01'
            FROM IN_RCYO tin
            WHERE tin.OCFECPROC = {cFormat.StToBD(cGlobales.Hoy) } ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Relaciones tratadas: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarRobcl", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSROBCL", "Error cargar tabla de Relaciones");
                return false;
            }
        }
    }
}

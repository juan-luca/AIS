using BusComun;
using Comun;
using System;


namespace BusInchost
{
    class CtaCteDalc : cBase
    {
        public CtaCteDalc(cConexion pconn)
        {
            conn = pconn;
        }

        internal bool VaciarCtaCte()
        {
            int nRet;
            String sSql;

            sSql = $"DELETE FROM RCCOR P WHERE EXISTS (SELECT 1 FROM IN_CCOR TIN WHERE P.CCOBLIG = TIN.CCOBLIG AND TIN.CCFECPROC = {cFormat.StToBD(cGlobales.Hoy)}) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarRCtaCte", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRUCtaCte", "Error al vaciar RCtaCte");
                return false;
            }
        }

        internal bool CargarCtaCte()
        {
            int nRet;

            string sSql = $@"INSERT INTO RCCOR (CCOBLIG, CCIMPORTE, CCSALDO, CCFECBAJA)
SELECT CCOBLIG, CCIMPORTE, CCSALDO, CCFECBAJA
FROM IN_CCOR 
WHERE CCFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Cuentas Corrientes tratadas: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarRCtaCte", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSCtaCte", "Error cargar tabla de CtaCte");
                return false;
            }
        }
    }
}

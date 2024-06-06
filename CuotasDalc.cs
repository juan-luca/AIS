using BusComun;
using Comun;
using System;


namespace BusInchost
{
    class CuotasDalc : cBase
    {
        public CuotasDalc(cConexion pconn)
        {
            conn = pconn;
        }

        internal bool VaciarCuotas()
        {
            int nRet;
            String sSql;

            sSql = $"DELETE FROM RCUOTAS P WHERE EXISTS (SELECT 1 FROM IN_CUOT TIN WHERE P.CUOBLIG = TIN.CUOBLIG AND TIN.CUFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarRCuotas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRUCuotas", "Error al vaciar RCuotas");
                return false;
            }
        }

        internal bool CargarCuotas()
        {
            int nRet;


            string sSql = $@"INSERT INTO RCUOTAS (CUFECVTO, CUESTADO, CUFECPAGO, CUOBLIG, CUTOTAL, CUNROCUOT, CUCAPITAL, CUINTCOR, CUIMPUEST)
SELECT CUFECVTO, CUESTADO, CUFECPAGO, CUOBLIG, COALESCE(CUCAPITAL, 0) + COALESCE(CUINTCOR,0) + COALESCE(CUIMPUEST,0) AS CUTOTAL, 
CUNROCUOT, CUCAPITAL, CUINTCOR, CUIMPUEST
FROM IN_CUOT
WHERE CUFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";


            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Cuotas tratadas: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarRCuotas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSCuotas", "Error cargar tabla de Cuotas");
                return false;
            }
        }
    }
}

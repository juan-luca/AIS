using BusComun;
using Comun;


namespace BusInchost
{
    class PrestamosDalc : cBase
    {
        public PrestamosDalc(cConexion pconn)
        {
            conn = pconn;
        }

        internal bool VaciarPrestamos()
        {
            int nRet;

            string sSql = $"DELETE FROM RPRES P WHERE EXISTS (SELECT 1 FROM IN_PRES TIN WHERE P.PROBLIG = TIN.PROBLIG  AND TIN.PRFECPROC =  {cFormat.StToBD(cGlobales.Hoy)} )";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarRPrestamos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRUPrestamos", "Error al vaciar RPrestamos");
                return false;
            }
        }

        internal bool CargarPrestamos()
        {
            int nRet;

            string sSql = $@"INSERT INTO RPRES (PROBLIG, PRCTA, PRFECVTO, PRNOTAS, PRCAPINI )
            SELECT PROBLIG, PRCTA, PRFECVTO, PRNOTAS, PRCAPINI
            FROM IN_PRES WHERE PRFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Prestamos tratados: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarRPrestamos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSPrestamos", "Error cargar tabla de Prestamos");
                return false;
            }
        }
    }
}

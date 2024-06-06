using BusComun;
using Comun;

namespace BusInchost
{
    public class CambioDalc : cBase
    {
        public CambioDalc(cConexion pconn)
        {
            conn = pconn;
        }

        /// <summary>
        /// Cerramos el periodo actual de las divisas que llegan por interfaz
        /// </summary>
        internal bool CerrarPeriodo()
        {
            int nRet;

            string sSql = $@"merge into RCAMBIO cam
using (
    SELECT CMESPECIE, CMCOTPESOS
    FROM IN_CAMBIO
    WHERE CMFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
) icam on (CAM.CMCOD = LTRIM(icam.CMESPECIE, '0') and CAM.CMVALOR <> icam.CMCOTPESOS ) 
when matched then 
UPDATE 
SET cam.CMHASTA = TO_CHAR(TO_DATE({cFormat.StToBD(cGlobales.Hoy)}, 'YYYYMMDD')-1, 'YYYYMMDD'), 
    cam.CMFECMODIF = {cFormat.StToBD(cGlobales.Hoy)}, 
    cam.CMHORAMODIF = {cFormat.StToBD(cGlobales.HoraMotor)}, 
    cam.CMUSRMODIF = -1 
where cam.cmhasta is null ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CerrarPeriodo", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRCAMBIO", "Error al Cerrar periodos RCLIDOC");
                return false;
            }
        }

        /// <summary>
        /// Insertamos los nuevos periodos para las moneda que han sufrido algún cambio
        /// Solo se están cargando las monedas '000001000','000002000','000002010'. El resto no se usan 
        /// </summary>
        /// <returns></returns>
        internal bool InsertarPeriodo()
        {
            int nRet;

            string sSql = $@"INSERT INTO RCAMBIO (CMCOD, CMDESDE, CMVALOR, CMUSRMODIF, CMFECMODIF, CMHORAMODIF)
select LTRIM(CMESPECIE, '0') CMESPECIE, {cFormat.StToBD(cGlobales.Hoy)} CMDESDE, CMCOTPESOS, -1 CMUSRMODIF, {cFormat.StToBD(cGlobales.Hoy)} CMFECMODIF, 
{cFormat.StToBD(cGlobales.HoraMotor)} CMHORAMODIF
from in_cambio inc
where CMFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
    AND inc.CMESPECIE IN ('000001000','000002000','000002010')
    AND NOT EXISTS (select 1 from rcambio p WHERE p.CMCOD = LTRIM(inc.CMESPECIE, '0') and p.CMHASTA is null) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Cotizaciones tratadas: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertarPeriodo", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSRCAMBIO", "Error al insertar RCAMBIO");
                return false;
            }

        }
    }
}

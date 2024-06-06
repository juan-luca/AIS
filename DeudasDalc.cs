using BusComun;
using Comun;
using System;

namespace BusInchost
{
    class DeudasDalc : cBase
    {
        public DeudasDalc(cConexion pconn)
        {
            conn = pconn;
        }

        internal bool VaciarRtDeuda()
        {
            int nRet;
            String sSql;

            sSql = "TRUNCATE TABLE RTDEUDA ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarRtDeuda", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRURTDEUDA", "Error vaciar temporal de deuda ");
                return false;
            }

        }

        internal bool ReplicarRTDeuda()
        {
            int nRet;

            string sSql = $@"INSERT INTO RTDEUDA 
            (TDOBLIG, TDDIAS, TDSALTOT, TDSALCAP, TDMORATOT, TDCAPITAL, TDINTCOR, TDIMPUEST, TDSALTOTUSD, TDMORATOTUSD, TDSALCAPUSD )
            SELECT DEOBLIG, DEDIAS, DESALTOT, DESALCAP, DEMORATOT, DECAPITAL, DEINTCOR, DEIMPUEST, DESALTOTUSD, DEMORATOTUSD, DESALCAPUSD 
            FROM RDEUDA ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ReplicarRTDeuda", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELRDEUDA", "Error al replicar la tabla de deuda");
                return false;
            }
        }

        internal bool CargarDeuda()
        {
            int nRet;
            String sSql;

            sSql = $" DELETE FROM RDEUDA de WHERE exists (SELECT 1 FROM IN_DEUD tin where tin.DEFECPROC = {cFormat.StToBD(cGlobales.Hoy)} and de.DEOBLIG = tin.DEOBLIG ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                sSql = $@"insert into RDEUDA(DEOBLIG, DESALTOT, DESALCAP, DEMORATOT, DECAPITAL, DEINTCOR, DEIMPUEST, DEULTACTUALIZACION)
select DEOBLIG, (DESALCAP + DEMORATOT) DESALTOT, DESALCAP, DEMORATOT, DECAPITAL, DEINTCOR, DEIMPUEST,DEFECPROC from IN_DEUD where DEFECPROC = { cFormat.StToBD(cGlobales.Hoy)}";

                nRet = conn.EjecutarQuery(sSql);
                if (conn.Errores.Cantidad() == 0)
                {
                    cIncidencia.Aviso($"Deudas tratadas: {nRet}");

                    //Actualizamos los días de mora de toda la deuda
                    sSql = $@"merge into RDEUDA de
using (
    select TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD'), 'YYYYMMDD') - TO_DATE(COALESCE(OGFECMOR, TO_CHAR(SYSDATE, 'YYYYMMDD')), 'YYYYMMDD') Dias, OGCOD
    from ROBLG
) og on (de.DEOBLIG = OG.OGCOD)
when matched then update set de.DEDIAS = og.Dias ";

                    nRet = conn.EjecutarQuery(sSql);
                    if (conn.Errores.Cantidad() == 0)
                    {
                        return true;
                    }
                    else
                    {
                        Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDeuda", Const.SEVERIDAD_Alta);
                        cIncidencia.Generar(Errores, "INSRDEUDA", "Error al actualizar los días de mora");
                        return false;
                    }
                }
                else
                {
                    Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDeuda", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "INSRDEUDA", "Error al cargar tabla de deuda");
                    return false;
                }
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDeuda", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELRDEUDA", "Error al vaciar tabla de deuda");
                return false;
            }

        }

        /// <summary>
        /// Actualizamos los campos USD de la deuda
        /// </summary>
        internal bool ActualizarDeudaDolar()
        {
            int nRet;

            // 1) para los contratos que están en USD movemos los importes de los campos de pesos a los de USD
            string sSql = $@"merge into RDEUDA de
using (
    select DEOBLIG, DESALTOT, DESALCAP, DEMORATOT, DECAPITAL, DEINTCOR, DEIMPUEST
    FROM RDEUDA
    JOIN ROBLG on OGCOD = DEOBLIG
    WHERE OGMONEDA IN ('2000','2010')
) og on(de.DEOBLIG = og.DEOBLIG)
when matched then update
set de.DESALTOTUSD = og.DESALTOT,
    de.DESALCAPUSD = og.DESALCAP, 
    de.DEMORATOTUSD = og.DEMORATOT,
    de.DECAPITALUSD = og.DECAPITAL, 
    de.DEINTCORUSD = og.DEINTCOR,
    de.DEIMPUESTUSD = og.DEIMPUEST";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
            {
                // 2) Ahora convertimos los campos USD a pesos y los almacenamos en las columnas de deuda en pesos
                sSql = $@"merge into RDEUDA de
using (
    select DEOBLIG, 
    ROUND(DESALTOT * CMVALOR,2) as DESALTOT, 
    ROUND(DESALCAP * CMVALOR,2) as DESALCAP, 
    ROUND(DEMORATOT * CMVALOR,2) as DEMORATOT, 
    ROUND(DECAPITAL * CMVALOR,2) as DECAPITAL, 
    ROUND(DEINTCOR * CMVALOR,2) as DEINTCOR, 
    ROUND(DEIMPUEST * CMVALOR,2) as DEIMPUEST
    FROM RDEUDA
    JOIN ROBLG on OGCOD = DEOBLIG
    JOIN RCAMBIO ON CMCOD = OGMONEDA AND CMDESDE <= {cFormat.StToBD(cGlobales.Hoy)} AND (CMHASTA IS NULL OR CMHASTA >= {cFormat.StToBD(cGlobales.Hoy)})
    WHERE OGMONEDA IN ('2000','2010')
) og on(de.DEOBLIG = og.DEOBLIG)
when matched then update
set de.DESALTOT = og.DESALTOT,
    de.DESALCAP = og.DESALCAP, 
    de.DEMORATOT = og.DEMORATOT,
    de.DECAPITAL = og.DECAPITAL, 
    de.DEINTCOR = og.DEINTCOR,
    de.DEIMPUEST = og.DEIMPUEST ";

                nRet = conn.EjecutarQuery(sSql);

                if (conn.Errores.Cantidad() == 0)
                    return true;
                else
                {
                    Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ReplicarRTDeuda", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "ActualizarDeudaDolar", "Error al Convertir de USD a Pesos ");
                    return false;
                }
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ReplicarRTDeuda", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "ActualizarDeudaDolar", "Error al traspasar importes de Pesos a USD ");
                return false;
            }
        }
    }
}

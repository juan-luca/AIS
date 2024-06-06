using BusComun;
using Comun;
using System.Data;

namespace BusInchost
{
    class TarjetasDalc : cBase
    {
        public TarjetasDalc(cConexion pconn)
        {
            conn = pconn;
        }

        internal bool VaciarTarjetas()
        {
            int nRet;

            string sSql = $"delete from RTARJ t where exists (select 1 from in_tarj ti where t.TAOBLIG = ti.TAOBLIG and ti.TAFECPROC = {cFormat.StToBD(cGlobales.Hoy)}) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarRTarjetas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRUTarjetas", "Error al vaciar RTarjetas");
                return false;
            }
        }

        internal bool CargarTarjetas()
        {
            int nRet;

            string sSql = $@"INSERT INTO RTARJ (TAOBLIG, TANUMCTA, TALIMCOM, TAESTADO, TASUCURSAL) 
            SELECT TAOBLIG, TANUMCTA, TALIMCOM, TAESTADO, REGEXP_REPLACE(TASUCURSAL, '^0+', '') as TASUCURSAL
            FROM IN_TARJ 
            WHERE TAFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Tarjetas tratadas: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarRTarjetas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSTarjetas", "Error cargar tabla de Tarjetas");
                return false;
            }
        }

        internal bool VaciarVtos()
        {
            int nRet;

            string sSql = $@"delete from rtarjvto t 
where exists (select 1 from IN_VENC ti where t.TVOBLIG = ti.TVOBLG AND t.TVFECVTO = ti.FECVTO and t.TVFECCIERR = ti.FECCIERR
and ti.TVFECPROC = {cFormat.StToBD(cGlobales.Hoy)}) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarVtosTarjetas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRUTarjetas", "Error al vaciar Vencimientos Tarjetas");
                return false;
            }
        }

        internal bool CargarVtos()
        {
            int nRet;

            string sSql = $@"insert into rtarjvto (TVOBLIG, TVFECVTO, TVFECCIERR, TVSALDO, TVSALDOUSD, TVPAGOMIN, TVHAYFECCIERR, TVFORMAPAGO, 
                            TVCAPITAL, TVINTERES, TVGASTOS, TVCOMISIONES)
                            SELECT TVOBLG, FECVTO, FECCIERR, TVSALDO, TVSALDOUSD, TVPAGOMIN, TVHAYFECCIERR, TVFORMAPAGO, TVCAPITAL, TVINTERES, TVGASTOS, TVCOMISIONES 
                            FROM IN_VENC WHERE TVFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Vencimientos tratados: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarVtosTarjetas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRUTarjetas", "Error al cargar Vencimientos Tarjetas");
                return false;
            }
        }

        internal bool RegistrarCambiosEstado()
        {
            bool retorno = false;
           
            string sSql = $@" insert into RBGES(BGLOTE, BGFECHA, BGHORA, BGTARREA, BGGESCLIE, BGIDGESTION, BGOBLIG, BGFIGURA,
            BGNMORANT, BGNMORNEW, BGESTESCENANT, BGESTESCENNEW, BGOWNANT, BGOWNNEW)
                          select 
                          CLCOD
                          ,{cFormat.StToBD(cGlobales.Hoy)}
                          ,{cFormat.StToBD(cGlobales.HoraMotor)}
                          ,case IN_TARJ.TAESTADO when 'N' then 'NORM'
                                                 when 'I' then 'INHI'
                                                 when 'M' then 'MORA'
                                                 when 'B' then 'BOLE'
                                                 when 'P' then 'ESJU'
                                                 when 'Q' then 'ABAN'
                                                 when 'R' then 'TREF'
                                                 when 'C' then 'TBAC'
                                                 when 'X' then 'TBAJ'
                                                 when 'T' then 'PEBO'
                                                 END 
                          ,'0' 
                        ,{cFormat.StToBD(cGlobales.Hoy)} 
                        || {cFormat.StToBD(cGlobales.HoraMotor)} 
                        || case IN_TARJ.TAESTADO when 'N' then 'NORM'
                                                 when 'I' then 'INHI'
                                                 when 'M' then 'MORA'
                                                 when 'B' then 'BOLE'
                                                 when 'P' then 'ESJU'
                                                 when 'Q' then 'ABAN'
                                                 when 'R' then 'TREF'
                                                 when 'C' then 'TBAC'
                                                 when 'X' then 'TBAJ'
                                                 when 'T' then 'PEBO'
                                                 END 
                   , IN_TARJ.TAOBLIG 
                   ,'NP', LONMORA, LONMORA, LOTURNO, LOTURNO, LOPERFIL, LOPERFIL
                 From RTARJ inner join IN_TARJ ON RTARJ.TAOBLIG = IN_TARJ.TAOBLIG and IN_TARJ.TAESTADO != RTARJ.TAESTADO
                            inner join ROBLG ON RTARJ.TAOBLIG = ROBLG.OGCOD
                            JOIN ROBCL ON OCOBLIG = OGCOD AND OCFIGURA = 'T01'
                            JOIN RCLIE ON CLCOD = OCRAIZ
                            INNER JOIN RLOTE ON LOCOD = OCRAIZ
                 where IN_TARJ.TAFECPROC= {cFormat.StToBD(cGlobales.Hoy)} ";


            int res = conn.EjecutarQuery(sSql);

            Errores = conn.Errores;

            if (Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "RegistrarCambiosEstado", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "RegistrarCambiosEstado", "Fecha: " + cGlobales.Hoy);
            }
            else
                retorno = true;

            return retorno;

        }


    }
}

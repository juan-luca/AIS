using BusComun;
using Comun;
using RSModel;
using System;
using System.Data;


namespace BusInchost
{
    class RefinanciacionDalc : cBase
    {
        public RefinanciacionDalc(cConexion pconn)
        {
            conn = pconn;
        }
        
        public DataSet GetRefinanciacionDeudaVariada()
        {
            string sSql = $@" SELECT DISTINCT RECOD, 
            (SELECT SUM(DESALTOT) FROM RREFIOBLG INNER JOIN RDEUDA ON DEOBLIG = ROOBLG WHERE ROCODREF = RECOD) AS SALDOTOTAL,
            REPAGOINICIAL, REPORQUITA, REPORCOM, REPROVSELLOS, REIVA, REPORTASA, REPORTASAPLANTILLA, REPRIMERVENC, REPLAZO, RETOTAREFIN
            FROM RREFINANCIACION
            INNER JOIN RREFIOBLG ON RECOD = ROCODREF
            INNER JOIN RDEUDA ON DEOBLIG = ROOBLG
            INNER JOIN RTABL ESTADO ON ESTADO.TBNUME = {(int)Catalogos.Estados_Refinanciaciones} AND ESTADO.TBCODE = REESTADO
            INNER JOIN RTABL ESTADOUNICO ON ESTADOUNICO.TBNUME = {(int)Catalogos.Estados_Refinanciaciones_Unicos} AND ESTADOUNICO.TBCODE = ESTADO.TBLINK
            WHERE DESALTOT != ROSALTOT AND ESTADOUNICO.TBLINK = '1' AND (REHISTORICO IS NULL OR REHISTORICO = '0')  ";

            DataSet ds = conn.EjecutarQuery(sSql, "REFINANCIACION");
            Errores = conn.Errores;

            return ds;
        }

        public bool ModificarRefinanciacion(string idRefi, double saldoTotal, double quita, double montoRefinanciado, double importeCuota, double montoComision, double tasa, double tasaPlantilla, double montoSellados, double montoPagare, double cft)
        {
            string sSql = $@" UPDATE RREFINANCIACION SET 
            REDEUDATOT = {cFormat.NumToBD(saldoTotal.ToString())}, REQUITA = {cFormat.NumToBD(quita.ToString())}, 
            RETOTAREFIN = {cFormat.NumToBD(montoRefinanciado.ToString())}, REIMPOCUOT = {cFormat.NumToBD(importeCuota.ToString())},
            REFECMODIF = {cFormat.StToBD(DateTime.Now.ToString("yyyyMMdd"))}, REIMPCOM = {cFormat.NumToBD(montoComision.ToString())},
            RETASA = {cFormat.NumToBD(tasa.ToString())}, RETASAPLANTILLA = {cFormat.NumToBD(tasaPlantilla.ToString())},
            RESELLOS = {cFormat.NumToBD(montoSellados.ToString())}, REPAGARE = {cFormat.NumToBD(montoPagare.ToString())},
            RECFT = {cFormat.NumToBD(cft.ToString())}
            WHERE RECOD = {cFormat.StToBD(idRefi)} ";

            this.conn.EjecutarScalar(sSql);
            this.Errores = conn.Errores;

            return this.Errores.Cantidad() == 0;
        }
        
        public bool ModificarCuotasRefinanciacion(string idRefi, int idCuota, double total, double capital, double interes, double seguro, double iva, double sellos, double saldoPend, double saldoPendCuota)
        {
            string sSql = $@" UPDATE RREFICUOTAS SET 
            RCTOTAL = {cFormat.NumToBD(total.ToString())}, RCCAPITAL = {cFormat.NumToBD(capital.ToString())},
            RCINTERES = {cFormat.NumToBD(interes.ToString())}, RCSEGURO = {cFormat.NumToBD(seguro.ToString())},
            RCIVA = {cFormat.NumToBD(iva.ToString())}, RCSELLOS = {cFormat.NumToBD(sellos.ToString())}, 
            RCSALDOPEND = {cFormat.NumToBD(saldoPend.ToString())}, RCSALDOPENDCUOTA = {cFormat.NumToBD(saldoPendCuota.ToString())}
            WHERE RCCOD = {cFormat.StToBD(idRefi)} AND RCCUOTA = {cFormat.NumToBD(idCuota.ToString())} ";

            this.conn.EjecutarScalar(sSql);
            this.Errores = conn.Errores;

            return this.Errores.Cantidad() == 0;
        }

        public bool ActualizarDeudaObligacionRefi(string idRefi)
        {
            string sSql = $@"
            UPDATE 
            (
                SELECT ROSALTOT, ROMORATOT, ROCAPITAL, ROINTCOR, ROIMPUEST, RODIAS, DESALTOT, DEMORATOT, DECAPITAL, DEINTCOR, 
                DEIMPUEST, DEDIAS
                FROM RREFIOBLG 
                INNER JOIN RDEUDA 
                ON ROOBLG = DEOBLIG
                WHERE ROCODREF = {cFormat.StToBD(idRefi)}
            ) DEUDAS
            SET ROSALTOT = DESALTOT, ROMORATOT = DEMORATOT, ROCAPITAL = DECAPITAL, ROINTCOR = DEINTCOR, ROIMPUEST = DEIMPUEST, RODIAS = DEDIAS ";

            this.conn.EjecutarScalar(sSql);
            this.Errores = conn.Errores;

            return this.Errores.Cantidad() == 0;
        }

        public DataSet TablaDeTablas(Catalogos numeroTabla, string nombreTabla, ColumnsTABLFilter columna, string valor, bool incluirBajas = false)
        {
            DataSet Ds;
            string baja = " AND RT.TBFECBAJA IS NULL ";

            if (incluirBajas)
                baja = "";

            string sSql = $@" SELECT RT.TBCODE CODIGO, RT.TBTEXT DESCRIPCION,  RT.TBABREV, RT.TBAIS, RT.TBAIS2, RT.TBBMODIF, RT.TBMULTIEMPRESA, RT.TBFECBAJA, 
                              MODIF.TBTEXT MODIFICABLE, RT.TBLINK TBLINK
                              FROM RTABL RT 
                              LEFT JOIN RTABL MODIF ON RT.TBBMODIF = MODIF.TBCODE AND  MODIF.TBNUME = 3 
                              WHERE RT.TBNUME = {(int)numeroTabla} AND RT.{columna.ToString()} = {cFormat.StToBD(valor)} {baja} ORDER BY RT.TBTEXT ";

            Ds = conn.EjecutarQuery(sSql, nombreTabla);

            this.Errores = conn.Errores;
            return Ds;
        }

        public bool ModificarRefinanciacion(string idRefi, double saldoTotal, double porQuita, double quita, double pagare, double cft)
        {
            string sSql = $@" UPDATE RREFINANCIACION 
                              SET REDEUDATOT = {cFormat.NumToBD(saldoTotal.ToString())}, REQUITA = {cFormat.NumToBD(quita.ToString())}, 
                              REPORQUITA = {cFormat.NumToBD(porQuita.ToString())}, REPAGARE = {cFormat.NumToBD(pagare.ToString())},
                              RECFT = {cFormat.NumToBD(cft.ToString())}
                              WHERE RECOD = {cFormat.StToBD(idRefi)} ";

            this.conn.EjecutarScalar(sSql);
            this.Errores = conn.Errores;

            return this.Errores.Cantidad() == 0;
        }

        public bool ModificarCuotaPorSellado(string idRefi, double sellado)
        {
            string sSql = $@" UPDATE RREFICUOTAS 
                              SET RCSELLOS = {cFormat.NumToBD(sellado.ToString())}, RCTOTAL = RCCAPITAL + RCINTERES + RCSEGURO + RCIVA + {cFormat.NumToBD(sellado.ToString())}
                              WHERE RCCOD = {cFormat.StToBD(idRefi)} AND RCCUOTA = 1 ";

            this.conn.EjecutarScalar(sSql);
            this.Errores = conn.Errores;

            return this.Errores.Cantidad() == 0;
        }

        public DataSet GetCuotas(string idRefi)
        {
            string sSql = $@" SELECT RCTOTAL
                              FROM RREFICUOTAS
                              WHERE RCCOD = {cFormat.StToBD(idRefi)} ";

            DataSet ds = this.conn.EjecutarQuery(sSql, "CUOTAS");
            this.Errores = conn.Errores;

            return ds;
        }
    }
}

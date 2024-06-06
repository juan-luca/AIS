using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class TyObligLote
    {
        public string OGCOD;
        public string OGTIPO;
        public string OGLOTE;
        public string OGLIDER;
        public string OGREGION;
        public string OGFECMOR;
        public string OGESTADO;
        public string OCRAIZ;
        public long DEDIAS;
        public double MORATOTAL;
        public double SALCAP;
        public string OGEMPRESA;
        public string OGCONVENIO;
        public string LOTEACTUAL;
        public double DEUDATOTAL;
        //public double DEUDATOTALUSD;
        public double DEMORATOTUSD;
        public double DESALCAPUSD;
    }

    public class TyLote
    {
        public string LOCOD;
        public string LOPERFIL;
        public string LOROL;
        public string LONMORA;
        public string LOTURNO;
        public int LONUMELEMS;
        public string LOESTRATEG;
        public double LOPRIORIDAD;
        public double LODEUDA;
        public double LODEUDAUSD;
        public double LODEUDATOTAL;
        public string LOFECALTA;
        public string LOFECPERFIL;
        public string LOFECROL;
        public string LOFECNMORA;
        public string LOFECTURNO;
        public string LOESTADO;
        public string LOESTADOESCEN;
    }

    class LotesDalc : cBase
    {
        #region Constructor

        public LotesDalc(cConexion pconn)
        {
            conn = pconn;
        }

        #endregion

        #region Proceso Masivo

        /// <summary>
        /// Se creará un nuevo Lote por cada clietne de tipo T01 en RCLIE que no tenga lote
        /// </summary>
        internal bool InsertarLotesNuevos()
        {
            string sSql = $@"INSERT INTO RLOTE (LOCOD,  LOESTADO, LOFECALTA) 
                            SELECT DISTINCT CLCOD LOCOD, 'A' LOESTADO, {cFormat.StToBD(cGlobales.Hoy)}
                            FROM RCLIE 
                            JOIN ROBCL ON OCRAIZ = CLCOD AND OCFIGURA = 'T01'
                            JOIN RMANDANTE M ON CLEMPRESA=M.MACOD
                            WHERE NOT EXISTS (SELECT 1 FROM RLOTE WHERE LOCOD = CLCOD) AND M.MAGRUPO='01' ";

            int nRes = conn.EjecutarQuery(sSql);


            // Logica mora tardia
            string sSqlLT = $@"INSERT INTO RLOTE (LOCOD,  LOESTADO, LOFECALTA) 
                            SELECT DISTINCT CLCOD LOCOD, 'A' LOESTADO, {cFormat.StToBD(cGlobales.Hoy)}
                            FROM RCLIE 
                            JOIN ROBCL ON OCRAIZ = CLCOD AND OCFIGURA = 'T01'
                            JOIN RMANDANTE M ON CLEMPRESA=M.MACOD
                            WHERE NOT EXISTS (SELECT 1 FROM RLOTE WHERE LOCOD = CLCOD) AND M.MAGRUPO='02' ";

            int nResLT = conn.EjecutarQuery(sSql);
            string sSqlTar = $@"INSERT INTO RLOTECLIE(LOCODCLIE, LOCODLOTE)
                                    SELECT distinct CLCOD, CONCAT(MAGRUPO, CLNUMDOC) AS LOTE
                                    FROM RCLIE
                                    INNER JOIN ROBCL ON OCRAIZ = CLCOD
                                    INNER JOIN RMANDANTE ON MAACTIVO = '1' AND MACOD = CLEMPRESA
                                    WHERE MAGRUPO = '02'
                                      AND NOT EXISTS (SELECT 1 FROM RLOTECLIE WHERE locodclie = CLCOD)";


            int nResTar = conn.EjecutarQuery(sSqlTar);

            if (conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(conn.Errores);
                return false;
            }
            else
            {
                cIncidencia.Aviso($"Lotes creados: {nRes} Lotes creados Mora Tardia:" + nResTar);
                return true;
            }
        }  

        /// <summary>
        /// Actualizamos campos resumen del Lote
        /// </summary>
        /// // JLS 05/06/2024 - Se agrega la logica para los lotes de mora tardia
        internal bool ActualizacionLotes()
        {
            string sSql = $@"MERGE INTO RLOTE 
            USING
            (
                SELECT OCRAIZ as OGLOTE, SUM(DEMORATOT) MORATOTAL, 
                    SUM(DESALCAP) SALCAP, 
                    SUM(DESALTOT) DEUDATOTAL, 
                    SUM(DEMORATOTUSD) DEMORATOTUSD, 
                    SUM(DESALCAPUSD) DESALCAPUSD, 
                    SUM(CASE WHEN DEDIAS > 0 THEN 1 ELSE 0 END) as NUMELEMS
                FROM ROBLG a
                JOIN ROBCL on OCOBLIG = OGCOD AND OCFIGURA = 'T01'
                JOIN RDEUDA on DEOBLIG = OGCOD
            WHERE NOT EXISTS (Select 1 from RLOTECLIE LC where LC.LOCODCLIE = OCRAIZ)
                GROUP BY OCRAIZ
            ) ON(LOCOD = OGLOTE)
            WHEN MATCHED THEN UPDATE
            SET LONUMELEMS = NUMELEMS,
                LODEUDA = MORATOTAL,
                LODEUDATOTAL = DEUDATOTAL,
                LODEUDAUSD = DEMORATOTUSD ";

            int nRes = conn.EjecutarQuery(sSql);
            string sSqlTard = $@"MERGE INTO RLOTE 
                USING
                (
                    SELECT LC.LOCODLOTE as OGLOTE, SUM(DEMORATOT) MORATOTAL, 
                        SUM(DESALCAP) SALCAP, 
                        SUM(DESALTOT) DEUDATOTAL, 
                        SUM(DEMORATOTUSD) DEMORATOTUSD, 
                        SUM(DESALCAPUSD) DESALCAPUSD, 
                        SUM(CASE WHEN DEDIAS > 0 THEN 1 ELSE 0 END) as NUMELEMS
                    FROM ROBLG a
                    JOIN ROBCL on OCOBLIG = OGCOD AND OCFIGURA = 'T01'
                    JOIN RDEUDA on DEOBLIG = OGCOD
                    JOIN RLOTECLIE LC ON LC.LOCODCLIE = OCRAIZ
                    GROUP BY LC.LOCODLOTE
                ) ON(LOCOD = OGLOTE)
                WHEN MATCHED THEN UPDATE
                SET LONUMELEMS = NUMELEMS,
                    LODEUDA = MORATOTAL,
                    LODEUDATOTAL = DEUDATOTAL,
                    LODEUDAUSD = DEMORATOTUSD ";

            int nResTard = conn.EjecutarQuery(sSqlTard);

            if (conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(conn.Errores);
                return false;
            }
            else
            {
                cIncidencia.Aviso($"Lotes temprana actualizados: {nRes} - Lotes tardia {nResTard} ");

                if (this.ActualizaNumElems())
                    return this.ActualizaDeuda();
                else
                    return false;
            }
        }

        private bool ActualizaNumElems()
        {
            string sSql = $"UPDATE RLOTE SET LONUMELEMS = 0 where LONUMELEMS is null";

            int nRes = conn.EjecutarQuery(sSql);

            if (this.conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(this.conn.Errores);
                return false;
            }
            else
                return true;
        }

        private bool ActualizaDeuda()
        {
            string sSql = $"UPDATE RLOTE SET LODEUDA = 0 where LODEUDA is null";

            int nRes = conn.EjecutarQuery(sSql);

            if (this.conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(this.conn.Errores);
                return false;
            }
            else
                return true;
        }

        /// <summary>
        /// Marcamos cual es la oblgiación lider
        /// La oblgiación lider será la que tenga fecha de mora mas antigua
        /// </summary>
        internal bool ObligacionesLider()
        {

            string sSql = $" UPDATE ROBLG SET OGLIDER = null, OGLOTE = null ";

            int nRes = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(conn.Errores);
                return false;
            }
            // JLS 05062024 - Se actualiza el LOTE contemplando los casos de mora tardia
            sSql = $@"MERGE INTO ROBLG OB USING
                (
                    select OGCOD, OCRAIZ as OGLOTE, 0 AS LIDER
                    FROM ROBLG
                    JOIN ROBCL on OCOBLIG = OGCOD AND OCFIGURA = 'T01'
                    JOIN RLOTE ON LOCOD = OCRAIZ
                    where NOT EXISTS (Select 1 from RLOTECLIE LC where LC.LOCODCLIE = OCRAIZ) 
                ) UP ON (OB.OGCOD = UP.OGCOD)
                WHEN MATCHED THEN UPDATE
                SET OB.OGLIDER = UP.LIDER, OB.OGLOTE = UP.OGLOTE ";

            nRes = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(conn.Errores);
                return false;
            }
            else
            {
                sSql = $@"MERGE INTO ROBLG OB USING
                (
                    select OGCOD, LC.LOCODLOTE as OGLOTE, 0 AS LIDER
                    FROM ROBLG
                    JOIN ROBCL on OCOBLIG = OGCOD AND OCFIGURA = 'T01'
                    JOIN RLOTE ON LOCOD = OCRAIZ
                    JOIN RLOTECLIE LC ON LC.LOCODCLIE = OCRAIZ 
                ) UP ON (OB.OGCOD = UP.OGCOD)
                WHEN MATCHED THEN UPDATE
                SET OB.OGLIDER = UP.LIDER, OB.OGLOTE = UP.OGLOTE ";

                nRes = conn.EjecutarQuery(sSql);


                if (conn.Errores.Cantidad() > 0)
                {
                    this.Errores.Agregar(conn.Errores);
                    return false;
                }
                else
                {
                    cIncidencia.Aviso($"Obligaciones inicializdas: {nRes}");
                sSql = $@"MERGE INTO ROBLG P
                        USING
                        (
                            SELECT OGCOD, OGLOTE
                            FROM
                            (
                                SELECT OGCOD, OGLOTE, 
                                    ROW_NUMBER() OVER(PARTITION BY OGLOTE ORDER BY DEDIAS DESC, DEMORATOT DESC, DESALTOT DESC, OGCOD ASC) AS numFila
                                FROM ROBLG
                                join robcl on ocoblig = ogcod AND OCRAIZ = oglote AND OCFIGURA = 'T01'
                                JOIN RDEUDA on DEOBLIG = OGCOD
                            )
                            WHERE numFila = 1
                        ) O ON (P.OGCOD = O.OGCOD)
                        WHEN MATCHED THEN UPDATE
                            SET OGLIDER = '1' ";

                nRes = conn.EjecutarQuery(sSql);

                if (conn.Errores.Cantidad() > 0)
                {
                    this.Errores.Agregar(conn.Errores);
                    return false;
                }
                else
                {
                    cIncidencia.Aviso($"Obligaciones marcadas como Lider: {nRes}");
                    return true;
                }
                }
            }
        }

        /// <summary>
        /// Asigna la prioridad de tratamiento de los lotes (ordenación agenda motor)
        /// </summary>
        internal bool AsignarPrioridadLote()
        {
            string sSql = $@"MERGE INTO RLOTE 
USING
(
    select OGLOTE, COALESCE(LODEUDATOTAL, 1) * COALESCE(DEDIAS, 1) PRIORIDAD
        from roblg 
        join RDEUDA on OGCOD = DEOBLIG 
        JOIN RLOTE on OGLOTE = LOCOD
        where OGLIDER = '1'
) ON(LOCOD = OGLOTE)
WHEN MATCHED THEN UPDATE
SET LOPRIORIDAD = PRIORIDAD ";

            int nRes = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(conn.Errores);
                return false;
            }
            else
            {
                cIncidencia.Aviso($"Prioridad actualizada para {nRes} lotes");
                return true;
            }
        }

        #endregion

        #region Proceso Unitario

        internal AISDataReader AbrirCursorObligaciones()
        {
            AISDataReader Dr = new AISDataReader();

            string sSql = $@" SELECT OGCOD, OGTIPO, OGLOTE, OGLIDER, OGREGION, OGFECMOR, OGESTADO, OCRAIZ, 
  DEDIAS, DEMORATOT MORATOTAL, DESALCAP SALCAP, DESALTOT as DEUDATOTAL, DEMORATOTUSD, DESALCAPUSD,
  OGEMPRESA, OGCONVENIO, LOCOD LOTEACTUAL
FROM ROBLG 
JOIN ROBCL on OGCOD = OCOBLIG AND OCFIGURA = 'T01'
JOIN RCLIE ON OCRAIZ = CLCOD
JOIN RDEUDA on DEOBLIG = OGCOD
JOIN RPRODUCTOS ON PDEMPRESA = OGEMPRESA  AND PDCOD = OGTIPO
LEFT JOIN RPRES ON PROBLIG = OGCOD
LEFT JOIN RLOTE ON LOCOD = OCRAIZ
ORDER BY OCRAIZ, DEDIAS DESC, DEMORATOT DESC, DESALTOT DESC ";

            //TEMPORAL FILTRADO DE CODIGO OBLIG WHERE  (ogcod like 'COM%' or OGCOD like 'LSG%')
#if DEBUG
            sSql += " FETCH FIRST 10 rows Only";
#endif

            Dr = conn.EjecutarDataReader(sSql);
            this.Errores = conn.Errores;

            return Dr;
        }

        internal bool FechObligaciones(AISDataReader DrC, ref TyObligLote Obligacion)
        {
            try
            {
                Obligacion = new TyObligLote();
                if (DrC.Read())
                {
                    Obligacion.OGCOD = DrC["OGCOD"].ToString();
                    Obligacion.OGTIPO = DrC["OGTIPO"].ToString();
                    Obligacion.OGLOTE = DrC["OGLOTE"].ToString();
                    Obligacion.OGLIDER = DrC["OGLIDER"].ToString();
                    Obligacion.OGREGION = DrC["OGREGION"].ToString();
                    Obligacion.OGFECMOR = DrC["OGFECMOR"].ToString();
                    Obligacion.OGESTADO = DrC["OGESTADO"].ToString();
                    Obligacion.OCRAIZ = DrC["OCRAIZ"].ToString();
                    Obligacion.DEDIAS = Convert.ToInt64(cFormat.NumBDToPC(DrC["DEDIAS"].ToString()));
                    Obligacion.MORATOTAL = cFormat.NumBDToPC(DrC["MORATOTAL"].ToString());
                    Obligacion.SALCAP = cFormat.NumBDToPC(DrC["SALCAP"].ToString());
                    Obligacion.OGEMPRESA = DrC["OGEMPRESA"].ToString();
                    Obligacion.OGCONVENIO = DrC["OGCONVENIO"].ToString();
                    Obligacion.LOTEACTUAL = DrC["LOTEACTUAL"].ToString();
                    Obligacion.DEUDATOTAL = cFormat.NumBDToPC(DrC["DEUDATOTAL"].ToString());
                    //Obligacion.DEUDATOTALUSD = cFormat.NumBDToPC(DrC["DEUDATOTALUSD"].ToString());
                    Obligacion.DEMORATOTUSD = cFormat.NumBDToPC(DrC["DEMORATOTUSD"].ToString());
                    Obligacion.DESALCAPUSD = cFormat.NumBDToPC(DrC["DESALCAPUSD"].ToString());

                    return true;
                }
                else
                    return false;
            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHOBLG", "Error al recuperar Obligacion");
                return false;
            }
        }

        internal bool UpdateLider(string sOblig, string sLote, bool bLider)
        {
            int nRet;

            string sSql = $@" UPDATE ROBLG SET 
  OGLOTE  = {cFormat.StToBD(sLote)}, 
  OGLIDER = {(bLider ? cFormat.StToBD("1") : cFormat.StToBD("0"))}
WHERE OGCOD = {cFormat.StToBD(sOblig)} ";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "UpdateLider", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRLOTELID", "Obligacion: " + sOblig);
                return false;
            }
        }

        internal bool ExisteLoteBD(string sCodLote)
        {
            int nCant = 0;

            DataSet Ds = new DataSet();

            string sSql = $" SELECT COUNT(*) CANT FROM RLOTE WHERE LOCOD = {cFormat.StToBD(sCodLote)} ";

            nCant = conn.EjecutarScalar(sSql);

            Errores = conn.Errores;

            return (nCant > 0);
        }

        internal bool InsertarLote(TyLote vLote)
        {
            int nRet;

            string sSql = $@" INSERT INTO RLOTE (LOCOD, LOPERFIL, LOROL, LONMORA, LOTURNO, LONUMELEMS, LOESTRATEG, LOPRIORIDAD, 
LODEUDA, LOFECALTA, LOFECPERFIL, LOFECROL, LOFECNMORA, LOFECTURNO, LOESTADO, LODEUDAUSD, LODEUDATOTAL ) 
VALUES ( 
  {cFormat.StToBD(vLote.LOCOD)}, {cFormat.StToBD(vLote.LOPERFIL)}, {cFormat.StToBD(vLote.LOROL)}, {cFormat.StToBD(vLote.LONMORA)},  
  {cFormat.StToBD(vLote.LOTURNO)}, {cFormat.NumToBD(vLote.LONUMELEMS.ToString())}, {cFormat.StToBD(vLote.LOESTRATEG)}, 
  {cFormat.NumToBD(vLote.LOPRIORIDAD.ToString())}, {cFormat.NumToBD(vLote.LODEUDA.ToString())}, {cFormat.StToBD(vLote.LOFECALTA)}, 
  {cFormat.StToBD(vLote.LOFECPERFIL)}, {cFormat.StToBD(vLote.LOFECROL)}, {cFormat.StToBD(vLote.LOFECNMORA)}, {cFormat.StToBD(vLote.LOFECTURNO)}, 
  {cFormat.StToBD(vLote.LOESTADO)}, {cFormat.NumToBD(vLote.LODEUDAUSD.ToString())}, {cFormat.NumToBD(vLote.LODEUDATOTAL.ToString())} ) ";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertarLote", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSRLOTE", "Error al insertar Lote: " + vLote.LOCOD);
                return false;
            }
        }

        internal bool UpdateCamposLote(TyLote vLote)
        {
            int nRet;

            string sSql = $@" UPDATE RLOTE SET 
    LONUMELEMS  = {cFormat.NumToBD(vLote.LONUMELEMS.ToString())},
    LOPRIORIDAD  = {cFormat.NumToBD(vLote.LOPRIORIDAD.ToString())}, 
    LODEUDA  = {cFormat.NumToBD(vLote.LODEUDA.ToString())}, 
    LODEUDAUSD = {cFormat.NumToBD(vLote.LODEUDAUSD.ToString())},
    LODEUDATOTAL = {cFormat.NumToBD(vLote.LODEUDATOTAL.ToString())},
    LOESTADO  = {cFormat.StToBD(vLote.LOESTADO)} 
WHERE LOCOD  = {cFormat.StToBD(vLote.LOCOD)} ";

            nRet = conn.EjecutarQuery(sSql);

            //cIncidencia.Aviso($" U_LOTE: {sSql}");

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "UpdateCamposLote", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRLOTE", "Error al actualizar Lote: " + vLote.LOCOD + " " + conn.Errores.Item(0).sDescripcion);
                return false;
            }
        }

        #endregion

        #region Estrategias

        internal DataSet GetAsigEstrategiasPendientes()
        {
            DataSet Ds = new DataSet();

            string sSql = $@" SELECT DISTINCT ETGRUPO, ETESTRATEG, ETPCT, 
             	ROUND((SELECT COUNT(*) 
             	FROM RRECESTR 
             	WHERE ESGRPEST = ETGRUPO) * ETPCT /100,0) DISTRIBUIR 
             FROM RESTRATEG, RRECESTR 
             where ETPCT > 0 
               and ETGRUPO = ESGRPEST 
             order by ETGRUPO, ETPCT desc ";

            Ds = conn.EjecutarQuery(sSql, "ASESTPDTE");

            if (conn.Errores.Cantidad() == 0)
                return Ds;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "GetAsigEstrategiasPendientes", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "GETESTGPDTE", "Error al recuperar asignaciones de estrategias");
                return Ds;
            }
        }

        internal bool InicializaEstrategiaLotes()
        {
            string sSql = @"update rlote 
       set loestrateg = null
       where not exists(SELECT ETESTRATEG FROM RESTRATEG where LOESTRATEG = ETESTRATEG)";
            conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "UpdateLider", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "InicializaEstrategiaLotes", "Error al inicializar estrategia lotes");
                return false;
            }
        }

        internal bool ActualizarEstrategia(string sGrupo, string sEstrategia, int nCantLote)
        {
            string sSql;
            int nRet;


            if (Const.MotorBD == ConstBD.MOTORORACLE)
            {
                sSql = "ALTER SESSION SET nls_sort = 'SPANISH'";
                conn.EjecutarQuery(sSql);

                sSql = "ALTER SESSION SET nls_comp = 'BINARY'";
                conn.EjecutarQuery(sSql);

                //sSql = $@" UPDATE RLOTE SET LOESTRATEG = {cFormat.StToBD(sEstrategia)}
                // WHERE EXISTS ( SELECT 1 
                //                  FROM RLOTE A 
                //                  WHERE EXISTS (SELECT * FROM RRECESTR 
                //                                WHERE ESLOTE = A.LOCOD AND ESGRPEST = {cFormat.StToBD(sGrupo)} ) 
                //                    AND 
                //                       ( 
                //                          EXISTS (SELECT * FROM RESTRATEG  
                //                                  WHERE ETESTRATEG = A.LOESTRATEG 
                //                                    AND ETGRUPO <> {cFormat.StToBD(sGrupo)}) 
                //                        OR 
                //                          NOT EXISTS (SELECT * FROM RESTRATEG 
                //                                      WHERE ETESTRATEG = A.LOESTRATEG) 
                //                        ) 
                //                    AND ROWNUM <= {nCantLote.ToString()} 
                //                    AND RLOTE.LOCOD = A.LOCOD ) ";


                sSql = $@" MERGE INTO RLOTE
                            USING(
                                 SELECT LOCOD
                                  FROM RLOTE A
                                  WHERE EXISTS(SELECT * FROM RRECESTR
                                                WHERE ESLOTE = A.LOCOD AND ESGRPEST =  { cFormat.StToBD(sGrupo)})
                                    AND
                                       (
                                          EXISTS(SELECT * FROM RESTRATEG
                                                  WHERE ETESTRATEG = A.LOESTRATEG
                                                    AND ETGRUPO <>  { cFormat.StToBD(sGrupo)})
                                            OR LOESTRATEG IS NULL
                                        )
                                and rownum <= {nCantLote}
                            ) A ON(RLOTE.LOCOD = A.LOCOD)
                            WHEN MATCHED THEN UPDATE
                            SET LOESTRATEG = { cFormat.StToBD(sEstrategia) }";
            }
            else
            {
                sSql = $@" UPDATE RLOTE SET LOESTRATEG = {cFormat.StToBD(sEstrategia)} 
                 WHERE RLOTE.LOCOD in ( SELECT TOP {nCantLote.ToString()} A.LOCOD 
                                  FROM RLOTE A 
                                  WHERE EXISTS (SELECT * FROM RRECESTR 
                                                WHERE ESLOTE = A.LOCOD AND ESGRPEST = {cFormat.StToBD(sGrupo)} ) 
                                    AND 
                                       ( 
                                          EXISTS (SELECT * FROM RESTRATEG  
                                                  WHERE ETESTRATEG = A.LOESTRATEG 
                                                    AND ETGRUPO <> { cFormat.StToBD(sGrupo)}) 
                                        OR 
                                          NOT EXISTS (SELECT * FROM RESTRATEG 
                                                      WHERE ETESTRATEG = A.LOESTRATEG) 
                                        ) 
                                    ) ";
            }

            cIncidencia.Aviso("Actlualizacion loestrateg lotes - " + DateTime.Now.ToString("HH:mm:ss:fff"));
            nRet = conn.EjecutarQuery(sSql);
            cIncidencia.Aviso("FIN Actlualizacion loestrateg lotes - " + DateTime.Now.ToString("HH:mm:ss:fff"));

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "UpdateLider", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRLOTEST", "Error al actualizar estrategias Grupo: " + sGrupo + " Estrategia: " + sEstrategia);
                return false;
            }

        }

        #endregion

        #region Obtencion Datos

        internal DataSet GetLote(string codCliente)
        {
            DataSet Ds = new DataSet();

            string sSql = $@" SELECT LOCOD, LOPERFIL, LONMORA, LOTURNO
            FROM RLOTE
            WHERE LOCOD = {cFormat.StToBD(codCliente)} ";

            Ds = conn.EjecutarQuery(sSql, "LOTE");

            if (this.conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "GetLote", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "GetLote", "Se ha producido un error al obtener las informaciones de lote");
            }

            return Ds;
        }

        #endregion
    }
}

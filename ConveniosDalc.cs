using BusComun;
using Comun;
using System;
using System.Data;
using System.Text;

namespace BusInchost
{
    public class ConveniosDalc : cBase
    {
        public ConveniosDalc(cConexion pconn)
        {
            conn = pconn;
        }

        #region METODOS UTILIZADOS POR Proceso Generacion Convenios de Pago

        public AISDataReader AbrirCursorNuevosConvPagoBD()
        {
            StringBuilder sb = new StringBuilder();
            AISDataReader Dr = new AISDataReader();

            //OJO FIJARSE LAS FECHA DE PROCESAMIENTO

            sb.Append(" SELECT  COCONV, COOBLGCONV,  SUBSTR(COLOTE,1,6) as EMPRESA ,");
            sb.Append(" COMONEDA,  COOFIC, COFECHA, COLOTE, PRTIPOPRODCONVENIO, COTEM, ");
            sb.Append(" TO_CHAR(ADD_MONTHS(TO_DATE(COFECHA,'YYYYMMDD'), COPLAZO),'YYYYMMDD') FECVENC");
            sb.Append(" FROM RCONVP, RPARAM ");
            sb.Append(" WHERE NOT EXISTS(SELECT 1 FROM ROBLG WHERE OGCOD = COOBLGCONV)");
            sb.Append(" AND COESTADO = '1'");

            Dr = conn.EjecutarDataReader(sb.ToString());
            Errores = conn.Errores;
            return Dr;
        }

        public bool FechConvenio(AISDataReader DrConvenio, TyConvenio Convenio)
        {
            try
            {
                if (DrConvenio.Read())
                {

                    Convenio.COCONV = cFormat.NumBDToPC(DrConvenio["COCONV"].ToString().Trim());
                    Convenio.COOBLGCONV = DrConvenio["COOBLGCONV"].ToString().Trim();
                    Convenio.EMPRESA = DrConvenio["EMPRESA"].ToString().Trim();
                    Convenio.COMONEDA = DrConvenio["COMONEDA"].ToString().Trim();
                    Convenio.COOFIC = DrConvenio["COOFIC"].ToString().Trim();
                    Convenio.COFECHA = DrConvenio["COFECHA"].ToString().Trim();
                    Convenio.FECVENC = DrConvenio["FECVENC"].ToString().Trim();
                    Convenio.COLOTE = DrConvenio["COLOTE"].ToString().Trim();
                    Convenio.PRTIPOPRODCONVENIO = DrConvenio["PRTIPOPRODCONVENIO"].ToString().Trim();
                    Convenio.COTEM = cFormat.NumBDToPC(DrConvenio["COTEM"].ToString().Trim());


                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHCONVENIO", "Error al recuperar Convenio");
                return false;
            }
        }

        public bool ObtengoRegionBD(TyConvenio Convenio)
        {
            try
            {
                StringBuilder sSql = new StringBuilder();

                DataSet Ds = new DataSet();

                sSql.Append(" SELECT OFREGION");
                sSql.Append(" FROM ROFIC");
                sSql.Append(" WHERE OFEMPRESA = '" + Convenio.EMPRESA + "'");
                sSql.Append("  AND OFCOD = '" + Convenio.COOFIC + "'");


                Ds = conn.EjecutarQuery(sSql.ToString(), "REGION");

                if (conn.Errores.Cantidad() > 0)
                {
                    Errores.Agregar(Const.ERROR_BASE_DATOS, conn.Errores.Item(0).sDescripcion, "ObtengoRegionBD", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "SUCREG", "Error al recuperar Region: EMPRESA: " + Convenio.EMPRESA.ToString() + "  OFICINA: " + Convenio.COOFIC);
                    return false;
                }
                else
                {
                    if (Ds.Tables[0].Rows.Count == 0)
                    {
                        cIncidencia.Generar("SUCREGNULL", "SUCREGNULL", "Convenio sin Region: EMPRESA: " + Convenio.EMPRESA.ToString() + "  OFICINA: " + Convenio.COOFIC);
                        return false;
                    }

                }



                Convenio.REGION = Ds.Tables["REGION"].Rows[0]["OFREGION"].ToString();


                return true;


            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHCONVENIO", "Error al recuperar Convenio");
                return false;
            }
        }

        public bool InsertoConvenio(TyConvenio convenio)
        {
            StringBuilder sSql = new StringBuilder();
            int nRet;

            sSql.Append(" INSERT INTO ROBLG (");
            sSql.Append("  OGCOD,      OGTIPO");
            sSql.Append(" , OGEMPRESA, OGMONEDA");
            sSql.Append(" , OGLOTE,     OGOFIC");
            sSql.Append(" , OGFECLEG,   OGFECVEN");
            sSql.Append(" , OGFECENT,   OGESTADO");
            sSql.Append(" , OGREGION");
            sSql.Append(" ) VALUES (	");

            sSql.Append("'" + convenio.COOBLGCONV + "' ,");
            sSql.Append("'" + convenio.PRTIPOPRODCONVENIO + "' ,");
            sSql.Append("'" + convenio.EMPRESA + "' ,");
            sSql.Append("'" + convenio.COMONEDA + "' ,");
            sSql.Append("'" + convenio.COLOTE + "' ,");
            sSql.Append("'" + convenio.COOFIC + "' ,");

            //ojo ver este dato preguntar
            sSql.Append("'" + convenio.COFECHA + "' ,"); // OGFECLEG 

            sSql.Append("'" + convenio.FECVENC + "' ,");
            sSql.Append("'" + convenio.FECENT + "' ,");
            sSql.Append("'A' ,");
            sSql.Append("'" + convenio.REGION + "')");



            nRet = conn.EjecutarQuery(sSql.ToString());
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertoConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSCONV", "Error insertando Convenio " + convenio.COCONV.ToString());
                return false;
            }
        }

        #endregion METODOS UTILIZADOS POR Proceso Generacion Convenios de Pago


        public bool MarcoConveniosPFinalizadosBD()
        {
            int nRet;

            string sSql = $@" UPDATE RCONVP SET 
COESTADO = {cFormat.StToBD(Const.MARCA_CONVP_CUMPLIDO)},
COUSRMODIF ='BATCH',
COFECMODIF = {cFormat.StToBD(cGlobales.Hoy)}
WHERE COESTADO = '1'
    AND COCONV NOT IN (SELECT DISTINCT CUCONV FROM RCUOTASCONVP WHERE CUSALDOPEND>0) ";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "MarcoConveniosPFinalizadosBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDMACONVPF", "Error al dar por cumplidos los convenios de pago");
                return false;
            }

        }
        public bool MarcoConveniosPBajadasRelacionesBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();

            sb.Append(" UPDATE RCONVP SET");
            sb.Append(" RCONVP.COESTADO ='" + Const.MARCA_CONVP_BAJA_CLIENTE + "',");
            sb.Append(" RCONVP.COUSRMODIF ='BATCH',");
            sb.Append(" RCONVP.COFECMODIF ='" + cGlobales.Hoy + "'");
            sb.Append(" WHERE RCONVP.COESTADO ='1' ");
            sb.Append(" AND ((NOT EXISTS(SELECT 1 FROM RCLIE WHERE CLCOD = RCONVP.COLOTE AND CLESTADO <>'B'))");
            sb.Append(" OR  (NOT EXISTS(SELECT 1 FROM ROBLG, RCONVPOBLG B WHERE B.COCONV = RCONVP.COCONV AND B.COOBLG = OGCOD ");
            sb.Append("           AND ( OGESTADO is null OR ( ");
            sb.Append("           OGESTADO <>  '" + Const.OBLG_MARCA_REGISTRO_BAJA + "'");
            sb.Append("           AND OGESTADO <>  '" + Const.OBLG_MARCA_BAJA_NORMALIZACION + "'");
            sb.Append("           AND OGESTADO <>  '" + Const.OBLG_MARCA_BAJA_JUDICIAL + "'");
            sb.Append("           AND OGESTADO <>  '" + Const.OBLG_MARCA_BAJA_REFINANCIACION + "'");
            sb.Append("           AND OGESTADO <>  '" + Const.OBLG_MARCA_BAJA_REFORMULACION + "'");
            sb.Append("           AND OGESTADO <>  '" + Const.OBLG_MARCA_BAJA_OTROS + "')))");
            sb.Append(" 	)) ");

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "MarcoConveniosPBajadasRelacionesBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDMAFINCONVP", "Error al dar por finalizados convenios de pago por baja de relacion cliente/obligacion");
                return false;
            }
        }
        public bool InsertarCuotasMorosasConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();

            sb.Append(" INSERT INTO RCUOTAS");
            sb.Append(" (CUOBLIG, CUNROCUOT, CUFECVTO, CUFECEMISION, CUTOTAL,");
            sb.Append(" CUCAPITAL, CUINTCOR, CUINTPUN, CUOTROS,");
            sb.Append(" CUFECSIS)");
            sb.Append(" SELECT CUOBLG, CUCUOTA, CUFECVENC, CUFECEMIS, CUSALDOPEND, CUSALDOPEND, 0,0,0,CUFECVENC");
            sb.Append(" FROM RCUOTASCONVP, RCONVP");
            sb.Append(" WHERE COCONV = CUCONV");
            sb.Append(" AND COESTADO = '1'");
            sb.Append(" AND CUFECVENC < '" + cGlobales.Hoy + "'");
            sb.Append(" AND CUSALDOPEND > 0 ");
            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertarCuotasMorosasConvPBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSRCUOTASCONVP", "Error al insertar cuotas de convenio de pago");
                return false;
            }
        }
        public bool InicializoDeudaConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();

            sb.Append(" INSERT INTO RDEUDA(DEOBLIG, DESALCAP, DESALTOT, DEDIAS, DECAPITAL,");
            sb.Append("            DEMORATOT, DEINTCOR, DEOTROS, DEPAGMIN, DEMONPREV, DEPAGARE_FECSUSC)");
            sb.Append(" SELECT COOBLGCONV, 0, 0, 0, 0, 0,  0, 0, 0, 0, ''");
            sb.Append(" FROM RCONVP");
            sb.Append(" WHERE COESTADO = '1' ");

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InicializoDeudaConvPBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSRDEUDACONVP", "Error al insertar deuda de convenio de pago");
                return false;
            }
        }
        /*OJO TESTEAR BIEN ESTE METODO*/
        public bool ActualizoSaldoDeudaConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();

            sb.Append(" UPDATE RDEUDA");
            sb.Append(" 	SET DESALCAP=(SELECT SUM(CUSALDOPEND)FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 	WHERE COCONV = CUCONV");
            sb.Append(" 	  AND COESTADO = '1'");
            sb.Append(" 	  AND CUSALDOPEND > 0");
            sb.Append(" 	  AND DEOBLIG = CUOBLG),");
            sb.Append(" 	DESALTOT=(SELECT SUM(CUSALDOPEND)FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 	WHERE COCONV = CUCONV");
            sb.Append(" 	  AND COESTADO = '1'");
            sb.Append(" 	  AND CUSALDOPEND > 0");
            sb.Append(" 	  AND DEOBLIG = CUOBLG)");
            sb.Append(" 	WHERE EXISTS(SELECT 1 FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 		 WHERE COCONV = CUCONV");
            sb.Append(" 	   AND COESTADO = '1'");
            sb.Append(" 	   AND CUSALDOPEND > 0");
            sb.Append(" 	   AND DEOBLIG = CUOBLG)");
            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizoSaldoDeudaConvPBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRDEUDACONVP1", "Error al actualizar deuda de convenio de pago");
                return false;
            }
        }
        public bool ActualizoMoraDeudaConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();

            sb.Append(" UPDATE RDEUDA");
            sb.Append(" SET DEDIAS = (SELECT MAX(TO_DATE(CUFECVENC,'YYYYMMDD')- TO_DATE('" + cGlobales.Hoy + "','YYYYMMDD'))");
            sb.Append(" 		FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 		WHERE COCONV = CUCONV");
            sb.Append(" 		  AND COESTADO = '1'");
            sb.Append(" 		  AND CUFECVENC < '" + cGlobales.Hoy + "'");
            sb.Append(" 		  AND CUSALDOPEND > 0");
            sb.Append(" 		  AND COOBLGCONV = DEOBLIG),");
            sb.Append(" DECAPITAL =  (SELECT SUM(CUSALDOPEND)");
            sb.Append(" 		FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 		WHERE COCONV = CUCONV");
            sb.Append(" 		  AND COESTADO = '1'");
            sb.Append(" 		  AND CUFECVENC < '" + cGlobales.Hoy + "'");
            sb.Append(" 		  AND CUSALDOPEND > 0");
            sb.Append(" 		  AND COOBLGCONV = DEOBLIG),");
            sb.Append(" DEMORATOT =  (SELECT SUM(CUSALDOPEND)");
            sb.Append(" 		FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 		WHERE COCONV = CUCONV");
            sb.Append(" 		  AND COESTADO = '1'");
            sb.Append(" 		  AND CUFECVENC < '" + cGlobales.Hoy + "'");
            sb.Append(" 		  AND CUSALDOPEND > 0");
            sb.Append(" 		  AND COOBLGCONV = DEOBLIG),");
            sb.Append(" DEPAGMIN =  (SELECT SUM(CUSALDOPEND)");
            sb.Append(" 		FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 		WHERE COCONV = CUCONV");
            sb.Append(" 		  AND COESTADO = '1'");
            sb.Append(" 		  AND CUFECVENC < '" + cGlobales.Hoy + "'");
            sb.Append(" 		  AND CUSALDOPEND > 0");
            sb.Append(" 		  AND COOBLGCONV = DEOBLIG)");
            sb.Append(" WHERE EXISTS(SELECT 1 FROM RCUOTASCONVP, RCONVP");
            sb.Append(" 		  WHERE COCONV = CUCONV");
            sb.Append(" 		AND COESTADO = '1'");
            sb.Append(" 		AND CUFECVENC < '" + cGlobales.Hoy + "'");
            sb.Append(" 		AND CUSALDOPEND > 0");
            sb.Append(" 			AND DEOBLIG = CUOBLG) ");

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizoMoraDeudaConvPBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRDEUDACONVP2", "Error al actualizar deuda de convenio de pago");
                return false;
            }
        }
        public bool InsertarRelacionesConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();

            sb.Append(" INSERT INTO ROBCL (OCOBLIG, OCRAIZ, OCFIGURA)");
            sb.Append(" 	SELECT COOBLGCONV, COLOTE, 'T01'");
            sb.Append(" 	FROM RCONVP");
            sb.Append(" 	WHERE COESTADO ='1' ");

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertarRelacionesConvPBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSROBCLCONVP", "Error al insertar relaciones de convenios de pago");
                return false;
            }
        }
        /*ya esta esta.*/
        public bool InicializoRPresConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();


            sb.Append(" INSERT INTO RPRES");
            sb.Append(" (PROBLIG, PRCAPINI, PRCANCUO, PRCANCVE, PRCANCVI)");
            sb.Append(" 	SELECT COOBLGCONV, COTOTAREFIN, COPLAZO,0,0");
            sb.Append(" 	FROM RCONVP");
            sb.Append(" 	WHERE COESTADO ='1' ");

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InicializoRPresConvPBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSRPRESCONVP1", "Error al insertar detalle prestamo de convenios de pago");
                return false;
            }
        }

        /* preguntar campo PROBLIG (en los dos query)por que no esta en ninguna de las tablas "RCONVP, RCUOTASCONVP"*/
        public bool ActualizoRPresConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();

            /* Total Cuotas vencidas*/
            sb.Append(" UPDATE RPRES");
            sb.Append(" SET PRCANCVE =(SELECT COUNT(*)");
            sb.Append("    FROM RCONVP, RCUOTASCONVP");
            sb.Append("    WHERE COESTADO ='1'");
            sb.Append(" 	 AND COOBLGCONV = CUOBLG");
            sb.Append(" 	 AND CUFECVENC< '" + cGlobales.Hoy + "'");
            sb.Append(" 	 AND CUOBLG = PROBLIG)");
            sb.Append(" WHERE EXISTS(SELECT 1");
            sb.Append("    FROM RCONVP, RCUOTASCONVP");
            sb.Append("    WHERE COESTADO ='1'");
            sb.Append(" 	 AND COOBLGCONV = CUOBLG");
            sb.Append(" 	 AND CUFECVENC< '" + cGlobales.Hoy + "'");
            sb.Append(" 	 AND CUOBLG = PROBLIG) ");

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
            {
                /*preguntar si esto es similar al condicional (SQLCODE==0) || (SQLCODE==SQL_NOT_FOUND)*/
                /* Total Cuotas vencidas impagas */
                sb = new StringBuilder();

                sb.Append(" UPDATE RPRES");
                sb.Append(" SET PRCANCVI =(SELECT COUNT(*)");
                sb.Append("    FROM RCONVP, RCUOTASCONVP");
                sb.Append("    WHERE COESTADO ='1'");
                sb.Append(" 	 AND COOBLGCONV = CUOBLG");
                sb.Append(" 	 AND CUFECVENC<'" + cGlobales.Hoy + "'");
                sb.Append(" 	 AND CUSALDOPEND > 0");
                sb.Append(" 	 AND CUOBLG = PROBLIG)");
                sb.Append(" WHERE EXISTS(SELECT 1");
                sb.Append("    FROM RCONVP, RCUOTASCONVP");
                sb.Append("    WHERE COESTADO ='1'");
                sb.Append(" 	 AND COOBLGCONV = CUOBLG");
                sb.Append(" 	 AND CUFECVENC<'" + cGlobales.Hoy + "'");
                sb.Append(" 	 AND CUSALDOPEND > 0");
                sb.Append(" 	 AND CUOBLG = PROBLIG) ");

                nRet = conn.EjecutarQuery(sb.ToString());

                if (conn.Errores.Cantidad() == 0)
                    return true;
                else
                {
                    Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizoRPresConvPBD", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "INSRPRESCONVP3", "Error al insertar detalle prestamo de convenios de pago");
                    return false;
                }


            }
            return true;
        }

        public DataSet AbrirCursorDatosOblgConvPModificar()
        {
            StringBuilder sb = new StringBuilder();
            DataSet Ds = new DataSet();

            //sb.Append(" SELECT COCONV, OGCOD, OGFECMOR, CONVERT(VARCHAR(8),DATEADD(day,DEDIAS * (-1),CONVERT(datetime,'" + cGlobales.Hoy + "')),112) FECHAMORA,");
            //sb.Append("     OGFECULTPAGO, OGMONULTPAGO,OGEMPRESA,OGOFIC, A.DEDIAS DIASMORA");
            sb.Append(" SELECT COCONV, OGCOD, OGFECMOR, TO_CHAR( (TO_DATE('" + cGlobales.Hoy + "','YYYYMMDD')- DEDIAS), 'YYYYMMDD') FECHAMORA,");
            sb.Append("     NULL, NULL,OGEMPRESA,OGOFIC, A.DEDIAS DIASMORA");
            sb.Append(" FROM RCONVP, ROBLG, RDEUDA A");
            sb.Append(" WHERE COESTADO = '1'");
            sb.Append(" AND OGCOD = COOBLGCONV");
            sb.Append(" AND DEOBLIG = OGCOD");
            sb.Append(" AND ((OGFECMOR IS NULL");
            sb.Append("  AND EXISTS(SELECT 1 FROM RDEUDA B WHERE B.DEOBLIG = OGCOD AND B.DEDIAS >0))");
            sb.Append(" OR (OGFECMOR IS NOT NULL");
            sb.Append("  AND EXISTS(SELECT 1 FROM RDEUDA B WHERE B.DEOBLIG = OGCOD AND B.DEDIAS =0))) ");

            Ds = conn.EjecutarQuery(sb.ToString(), "CONPMOD");

            Errores = conn.Errores;
            return Ds;


        }

        public bool VaciarTablaTemporalCambioConvPBD()
        {
            int nRet;
            StringBuilder sb = new StringBuilder();


            sb.Append(" TRUNCATE TABLE RCAMBIOCONVP");

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarTablaTemporalCambioConvPBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "TRUNRCAMBIOCONVP", "Error al vaciar tabla temporal cambio convenio");
                return false;
            }
        }


        public AISDataReader AbrirCursorBajaConvP()
        {
            StringBuilder sb = new StringBuilder();
            AISDataReader Dr = new AISDataReader();

            sb.Append(" SELECT DISTINCT COCONV, COOBLGCONV, COESTADO, COLOTE");
            sb.Append(" FROM RCAMBIOCONVP, RCONVP, ROBLG");
            sb.Append(" WHERE CCCONV = COCONV");
            sb.Append(" AND OGCOD = COOBLGCONV ");

            Dr = conn.EjecutarDataReader(sb.ToString());
            Errores = conn.Errores;
            return Dr;
        }



        public bool FetchBajaConvP(AISDataReader DrConvenio, TyConvenio Convenio)
        {
            try
            {
                if (DrConvenio.Read())
                {

                    Convenio.COCONV = cFormat.NumBDToPC(DrConvenio["COCONV"].ToString().Trim());
                    Convenio.COOBLGCONV = DrConvenio["COOBLGCONV"].ToString().Trim();
                    Convenio.COESTADO = DrConvenio["COESTADO"].ToString().Trim();
                    Convenio.COLOTE = DrConvenio["COLOTE"].ToString().Trim();
                    //si el campo DrConvenio["COESTADO"] es nulo poner un ""

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELBAJACONVP", "Error en la carga de datos");
                return false;
            }
        }

        public bool FetchDatosOblgConvPModificar(AISDataReader DrConvenioObligacion, TyDatConv vDatObg)
        {
            try
            {
                if (DrConvenioObligacion.Read())
                {
                    /*OJO COMPROBAR FECHA MORA*/
                    vDatObg.Conv = cFormat.NumBDToPC(DrConvenioObligacion["COCONV"].ToString().Trim());
                    vDatObg.Oblig = DrConvenioObligacion["OGCOD"].ToString().Trim();
                    vDatObg.FechaMoraAnt = DrConvenioObligacion["OGFECMOR"].ToString().Trim();
                    vDatObg.FechaMora = DrConvenioObligacion["FECHAMORA"].ToString().Trim();
                    vDatObg.FechaUltPago = DrConvenioObligacion["OGFECULTPAGO"].ToString().Trim();
                    vDatObg.MonUltPago = cFormat.NumBDToPC(DrConvenioObligacion["OGMONULTPAGO"].ToString().Trim());
                    vDatObg.Mandante = DrConvenioObligacion["OGMANDANTE"].ToString().Trim();
                    vDatObg.Ofic = DrConvenioObligacion["OGOFIC"].ToString().Trim();

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FetchDatosOblgConvPModificar", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELDATOSOBLGCONVPMODIFICAR", "Error en la carga de datos");
                return false;
            }
        }



        public void SacoObligacionesOriginalesDelConvenioBD(double dConv)
        {

            int nRet;
            StringBuilder sb = new StringBuilder();

            sb.Append(" UPDATE ROBLG SET");
            sb.Append(" OGCONVENIO = null ");
            sb.Append(" WHERE OGCONVENIO = " + dConv);

            nRet = conn.EjecutarQuery(sb.ToString());

            if (conn.Errores.Cantidad() != 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "SacoObligacionesOriginalesDelConvenioBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDMAOGOR", "Error al desmarcar obligaciones originales del convenios");
            }


        }





        internal TyDatConv CargoOblgConvPModificar(DataRow Row)
        {
            try
            {
                TyDatConv vDatObg = new TyDatConv
                {
                    Conv = cFormat.NumBDToPC(Row["COCONV"].ToString().Trim()),
                    Oblig = Row["OGCOD"].ToString().Trim(),
                    FechaMoraAnt = Row["OGFECMOR"].ToString().Trim(),
                    FechaMora = Row["FECHAMORA"].ToString().Trim(),
                    FechaUltPago = Row["OGFECULTPAGO"].ToString().Trim(),
                    MonUltPago = cFormat.NumBDToPC(Row["OGMONULTPAGO"].ToString().Trim()),
                    Mandante = Row["OGEMPRESA"].ToString().Trim(),
                    Ofic = Row["OGOFIC"].ToString().Trim()
                };

                if (Row["DIASMORA"].ToString().Trim() == "0")
                {
                }

                return vDatObg;
            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "CargoOblgConvPModificar", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELDATOSOBLGCONVPMODIFICAR", "Error en la carga de datos");
                return new TyDatConv(); ;
            }
        }
    }

    public class TyConvenio
    {
        public double COCONV;
        public string COOBLGCONV;
        public string COLOTE;
        public string COFECHA;
        public string COOFIC;
        public string COMONEDA;
        public double CODEUDATOT;
        public double COPAGOINICIAL;
        public double CODESCUENTOS;
        public double COTOTAREFIN;
        public double COTEM;//TASA DEL CONVENIO
        public int COPLAZO;
        public string COPRIMERVENC;
        public string COESTADO;
        public string COUSRMODIF;
        public string COFECMODIF;
        public string COHISTORICO;

        public string EMPRESA;
        public string FECVENC;
        public string REGION;
        public string PRTIPOPRODCONVENIO;

        public string FECENT;


    }
    public class TyDatConv
    {
        public double Conv;
        public string Oblig;
        public string FechaMoraAnt;
        public string FechaMora;
        public string FechaUltPago;
        public double MonUltPago;
        public string Mandante;
        public string Ofic;

    }
}
